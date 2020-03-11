use bytes::BytesMut;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use futures::{SinkExt, StreamExt};

use rsds::protocol::protocol::{asyncwrite_to_sink, DaskPacket, Frame, asyncread_to_stream};
use std::fs::OpenOptions;
use std::io::Write;
use std::time::Duration;
use tokio::fs::File;
use tokio::runtime;
use tokio_util::codec::Encoder;
use tempfile::NamedTempFile;
use rsds::protocol::codec::DaskProto;
use tokio::runtime::Runtime;

fn create_bytes(size: usize) -> BytesMut {
    BytesMut::from(vec![0u8; size].as_slice())
}

fn serialize_packet(packet: DaskPacket) -> BytesMut {
    let mut bytes = Vec::default();
    let mut codec = DaskProto::new(&mut bytes);
    let mut rt = Runtime::new().unwrap();
    rt.block_on(async move {
        codec.send(packet).await.unwrap();
    });

    BytesMut::from(bytes.as_slice())
}

fn decode(c: &mut Criterion) {
    let mut group = c.benchmark_group("Decode");
    group.warm_up_time(Duration::from_secs(1));
    group.sample_size(10);

    let sizes = vec![
        256,
        1024,
        8 * 1024,
        64 * 1024,
        128 * 1024,
        1024 * 1024,
        32 * 1024 * 1024,
        256 * 1024 * 1024,
    ];
    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("Stream", size), &size, |b, &size| {
            let mut rt = runtime::Builder::new()
                .basic_scheduler()
                .enable_io()
                .build()
                .unwrap();

            let mut packet_file = NamedTempFile::new().unwrap();
            let bytes = serialize_packet(DaskPacket::new(
                Frame::from(vec![0u8; size].as_slice()),
                vec![],
            ));
            packet_file.write(&bytes).unwrap();

            b.iter_with_setup(
                || {
                    let file = File::from_std(packet_file.reopen().unwrap());
                    DaskProto::new(file)
                },
                |mut stream| {
                    rt.block_on(async move {
                        stream.next().await.unwrap().unwrap();
                    });
                },
            );
        });
    }
    group.finish();
}
fn encode(c: &mut Criterion) {
    let mut group = c.benchmark_group("Encode");
    group.warm_up_time(Duration::from_secs(1));
    group.sample_size(10);

    let sizes = vec![
        256,
        1024,
        8 * 1024,
        64 * 1024,
        128 * 1024,
        1024 * 1024,
        32 * 1024 * 1024,
        256 * 1024 * 1024,
    ];
    for size in sizes {
        group.throughput(Throughput::Bytes(size as u64));
        group.bench_with_input(BenchmarkId::new("Sink", size), &size, |b, &size| {
            let mut rt = runtime::Builder::new()
                .basic_scheduler()
                .enable_io()
                .build()
                .unwrap();

            b.iter_with_setup(
                || {
                    let file =
                        File::from_std(OpenOptions::new().write(true).open("/dev/null").unwrap());
                    let sink = asyncwrite_to_sink(file);

                    let packet = DaskPacket::new(create_bytes(size), vec![]);
                    (sink, packet)
                },
                |(mut sink, packet)| {
                    rt.block_on(async move {
                        sink.send(packet).await.unwrap();
                    });
                },
            );
        });
    }
    group.finish();
}

criterion_group!(protocol, encode, decode);
criterion_main!(protocol);
