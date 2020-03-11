use crate::protocol::protocol::{DaskPacket, Frame, Frames};
use crate::trace::{trace_packet_receive, trace_packet_send};
use byteorder::{LittleEndian, ReadBytesExt};
use bytes::{Buf, BufMut, BytesMut};
use futures::task::{Context, Poll};
use futures::{Sink, Stream};
use pin_project_lite::pin_project;
use std::mem::MaybeUninit;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::macros::support::Pin;

macro_rules! ready_stream {
    ($e:expr $(,)?) => {
        match $e {
            ::std::task::Poll::Ready(t) => match t {
                Ok(val) => {
                    if val == 0 {
                        return ::std::task::Poll::Ready(None);
                    } else {
                        val
                    }
                },
                Err(e) => return ::std::task::Poll::Ready(Some(Err(e.into()))),
            },
            ::std::task::Poll::Pending => return ::std::task::Poll::Pending,
        }
    };
}
macro_rules! ready {
    ($e:expr $(,)?) => {
        match $e {
            ::std::task::Poll::Ready(t) => match t {
                Ok(val) => val,
                Err(e) => return ::std::task::Poll::Ready(Err(e.into())),
            },
            ::std::task::Poll::Pending => return ::std::task::Poll::Pending,
        }
    };
}

pin_project! {
    pub struct DaskProto<T> {
        #[pin]
        inner: T,
        #[pin]
        decoder: Decoder,
        #[pin]
        encoder: Encoder
    }
}

impl<T> DaskProto<T> {
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            decoder: Decoder::default(),
            encoder: Encoder::default(),
        }
    }
    pub fn into_inner(self) -> T {
        self.inner
    }
}

impl<T: AsyncRead> Stream for DaskProto<T> {
    type Item = crate::Result<DaskPacket>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        this.decoder.poll_next(this.inner, cx)
    }
}

enum DecoderState {
    Header,
    Payload { frame_index: usize },
}
impl Default for DecoderState {
    fn default() -> Self {
        Self::Header
    }
}

struct Decoder {
    header_buffer: BytesMut,
    main_frame: Frame,
    additional_frames: Frames,
    state: DecoderState,
}

type Endianness = LittleEndian;

impl Decoder {
    fn default() -> Self {
        Self {
            header_buffer: BytesMut::with_capacity(512),
            main_frame: Default::default(),
            additional_frames: Default::default(),
            state: Default::default(),
        }
    }

    fn construct_packet(&mut self) -> DaskPacket {
        let packet = DaskPacket::new(
            std::mem::take(&mut self.main_frame),
            std::mem::take(&mut self.additional_frames),
        );
        self.state = Default::default();
        packet
    }

    fn poll_next<T: AsyncRead>(
        &mut self,
        mut reader: Pin<&mut T>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<crate::Result<DaskPacket>>> {
        if let DecoderState::Header = &self.state {
            let buffer = &mut self.header_buffer;
            while buffer.len() < 8 {
                ready_stream!(read_bytes(reader.as_mut(), cx, buffer));
            }
            let mut cursor = std::io::Cursor::new(buffer);
            // Following read_u64 cannot failed, hence do not propagate and leave .unwrap() here
            let count: u64 = cursor.read_u64::<Endianness>().unwrap();

            let buffer = cursor.into_inner();
            let header_size = (count + 1) * 8;
            buffer.reserve(header_size as usize);
            while buffer.len() < header_size as usize {
                ready_stream!(read_bytes(reader.as_mut(), cx, buffer));
            }
            buffer.advance(8);
            let mut cursor = std::io::Cursor::new(buffer);
            let first_size = cursor.read_u64::<Endianness>().unwrap();
            assert_eq!(first_size, 0);

            let main_size = cursor.read_u64::<Endianness>().unwrap();
            // preallocate space
            std::mem::replace(
                &mut self.main_frame,
                BytesMut::with_capacity(main_size as usize),
            );
            self.additional_frames.clear();

            let mut total_size: u64 = main_size + header_size;
            for _ in 2..count {
                let size = cursor.read_u64::<Endianness>().unwrap();
                self.additional_frames
                    .push(BytesMut::with_capacity(size as usize));
                total_size += size;
            }
            trace_packet_receive(total_size as usize);

            self.state = DecoderState::Payload { frame_index: 0 };
            let buffer = cursor.into_inner();
            buffer.advance((header_size - 8) as usize);
        }

        if let DecoderState::Payload {
            ref mut frame_index,
        } = &mut self.state
        {
            loop {
                let buffer = match *frame_index {
                    0 => &mut self.main_frame,
                    index => &mut self.additional_frames[index - 1],
                };
                let buf_remaining = buffer.capacity() - buffer.len();
                if buf_remaining == 0 {
                    if *frame_index == self.additional_frames.len() {
                        return Poll::Ready(Some(Ok(self.construct_packet())));
                    } else {
                        *frame_index += 1;
                        continue;
                    }
                }
                let buf_available = self.header_buffer.len();
                if buf_available > 0 {
                    let to_copy = std::cmp::min(buf_remaining, buf_available);
                    buffer.extend_from_slice(&self.header_buffer.as_ref()[..to_copy]);
                    self.header_buffer.advance(to_copy);
                } else {
                    ready_stream!(read_bytes(reader.as_mut(), cx, buffer));
                }
            }
        }
        Poll::Pending
    }
}

fn read_bytes<T: AsyncRead>(
    reader: Pin<&mut T>,
    cx: &mut Context<'_>,
    bytes: &mut BytesMut,
) -> Poll<std::io::Result<usize>> {
    if !bytes.has_remaining_mut() {
        return Poll::Ready(Ok(0));
    }

    let b = bytes.bytes_mut();
    let b = unsafe { &mut *(b as *mut [MaybeUninit<u8>] as *mut [u8]) };
    let n = ready!(reader.poll_read(cx, b));
    assert!(n <= b.len());
    unsafe { bytes.advance_mut(n); }
    Poll::Ready(Ok(n))
}

impl<T: AsyncWrite> Sink<DaskPacket> for DaskProto<T> {
    type Error = crate::DsError;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        if self.encoder.buffer.len() >= 8 * 1024 {
            match self.as_mut().poll_flush(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => (),
            };

            if self.encoder.buffer.len() >= 8 * 1024 {
                return Poll::Pending;
            }
        }
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: DaskPacket) -> Result<(), Self::Error> {
        let mut encoder = self.project().encoder;
        assert!(encoder.packet.is_none());

        let frames = item.frame_count();
        let header_bytes = item.header_bytes();
        let payload_bytes = item.payload_bytes();
        let total_bytes = header_bytes + payload_bytes;

        trace_packet_send(total_bytes);

        let buffered = total_bytes < 64 * 1024;

        let dst = &mut encoder.buffer;
        dst.reserve(if buffered { total_bytes } else { header_bytes });
        dst.put_u64_le(frames as u64);
        dst.put_u64_le(0);
        dst.put_u64_le(item.main_frame.len() as u64);
        for frame in &item.additional_frames {
            dst.put_u64_le(frame.len() as u64);
        }

        if buffered {
            dst.extend_from_slice(item.main_frame.as_ref());
            for frame in &item.additional_frames {
                dst.extend_from_slice(frame.as_ref());
            }
            encoder.send_direct = false;
        } else {
            encoder.send_direct = true;
        }

        encoder.packet = Some(item);
        encoder.frame_index = 0;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        let mut this = self.project();
        this.encoder.poll_flush(this.inner, cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx));
        ready!(self.project().inner.poll_shutdown(cx));
        Poll::Ready(Ok(()))
    }
}

struct Encoder {
    packet: Option<DaskPacket>,
    send_direct: bool,
    buffer: BytesMut,
    frame_index: usize,
}

impl Default for Encoder {
    fn default() -> Self {
        Self {
            packet: None,
            send_direct: false,
            buffer: BytesMut::with_capacity(8 * 1024),
            frame_index: 0
        }
    }
}

impl Encoder {
    fn poll_flush<T: AsyncWrite>(
        &mut self,
        mut writer: Pin<&mut T>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), crate::DsError>> {
        if self.packet.is_none() {
            return Poll::Ready(Ok(()));
        }

        while self.buffer.len() > 0 {
            let n = ready!(writer.as_mut().poll_write(cx, &self.buffer));
            assert!(n > 0);
            self.buffer.advance(n);
        }
        if !self.send_direct {
            ready!(writer.as_mut().poll_flush(cx));
            self.packet = None;
            return Poll::Ready(Ok(()));
        }

        let packet = self.packet.as_mut().unwrap();
        loop {
            let buffer = match self.frame_index {
                0 => &mut packet.main_frame,
                index => &mut packet.additional_frames[index - 1],
            };
            let buf_remaining = buffer.len();
            if buf_remaining == 0 {
                if self.frame_index == packet.additional_frames.len() {
                    ready!(writer.as_mut().poll_flush(cx));
                    self.packet = None;
                    return Poll::Ready(Ok(()));
                } else {
                    self.frame_index += 1;
                    continue;
                }
            }
            let n = ready!(writer.as_mut().poll_write(cx, &buffer));
            assert!(n > 0);
            buffer.advance(n);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::protocol::codec::DaskProto;
    use bytes::{BufMut, BytesMut};
    use futures::{StreamExt, SinkExt};
    use std::io::Cursor;
    use crate::protocol::protocol::DaskPacket;

    #[tokio::test]
    async fn test2() {
        let packet = DaskPacket::new(BytesMut::from([1u8, 2u8, 3u8].as_ref()), vec!(BytesMut::from([4u8, 5u8, 6u8].as_ref()), BytesMut::from([7u8, 8u8, 9u8].as_ref())));
        let sink = tokio::io::sink();
        let mut codec = DaskProto::new(sink);
        codec.send(packet.clone()).await.unwrap();
        codec.send(packet).await.unwrap();
    }

    #[tokio::test]
    async fn test() {
        let mut buf = BytesMut::default();
        let sizes: Vec<usize> = vec![13, 17, 2, 1];

        for i in 0..3 {
            buf.reserve(8 + 8 * (2 + sizes.len()) + 1 + sizes.iter().sum::<usize>());
            buf.put_u64_le((2 + sizes.len()) as u64);
            buf.put_u64_le(0);
            buf.put_u64_le(1);
            for &size in sizes.iter() {
                buf.put_u64_le(size as u64);
            }
            buf.put_u8(137u8);
            for &size in sizes.iter() {
                buf.put_slice(
                    &std::iter::repeat(size as u8)
                        .take(size)
                        .collect::<Vec<u8>>(),
                );
            }
        }

        let cursor = Cursor::new(buf.to_vec());
        let mut proto = DaskProto::new(cursor);
        for i in 0..3 {
            let packet = proto.next().await.unwrap().unwrap();
            assert_eq!(packet.main_frame.to_vec(), vec!(137u8));
            assert_eq!(packet.additional_frames.len(), sizes.len());
            for (&size, frame) in sizes.iter().zip(packet.additional_frames.into_iter()) {
                assert_eq!(frame.len(), size);
                assert_eq!(
                    frame.to_vec(),
                    std::iter::repeat(size)
                        .take(size)
                        .map(|i| i as u8)
                        .collect::<Vec<u8>>()
                );
            }
        }
    }
}
