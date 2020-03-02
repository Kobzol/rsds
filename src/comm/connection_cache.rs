use crate::common::{Map, WrappedRcRefCell};
use crate::protocol::key::{DaskKey, DaskKeyRef, dask_key_ref_to_string};
use tokio::net::TcpStream;
use std::ops::DerefMut;
use tokio::io::AsyncRead;
use futures::task::{Context, Poll};
use tokio::macros::support::Pin;
use tokio::io::AsyncWrite;

pub type ConnectionRef = WrappedRcRefCell<TcpStream>;
pub type ConnectionCacheRef = WrappedRcRefCell<ConnectionCache>;

#[derive(Debug)]
pub struct SharedConnection {
    stream: ConnectionRef,
    address: DaskKey,
}

impl AsyncRead for SharedConnection {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(self.stream.get_mut().deref_mut()).poll_read(cx, buf)
    }
}
impl AsyncWrite for SharedConnection {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        Pin::new(self.stream.get_mut().deref_mut()).poll_write(cx, buf)
    }
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(self.stream.get_mut().deref_mut()).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(self.stream.get_mut().deref_mut()).poll_shutdown(cx)
    }
}


#[derive(Default)]
pub struct ConnectionCache {
    connections: Map<DaskKey, Vec<ConnectionRef>>,
}

impl ConnectionCacheRef {
    pub async fn get_connection(&self, address: &DaskKeyRef) -> crate::Result<SharedConnection> {
        let existing_connection = {
            let mut selfref = self.get_mut();
            if !selfref.connections.contains_key(address) {
                selfref
                    .connections
                    .insert(address.into(), Default::default());
            }
            selfref.connections.get_mut(address).unwrap().pop()
        };
        let stream = match existing_connection {
            Some(stream) => stream,
            None => Self::connect(address).await?,
        };
        Ok(SharedConnection {
            stream,
            address: address.into()
        })
    }

    pub fn return_connection(&self, connection: SharedConnection) {
        let mut selfref = self.get_mut();
        let pool = selfref
            .connections
            .get_mut(&connection.address)
            .expect("Address not present in connection cache");
        pool.push(connection.stream);
    }

    async fn connect(address: &DaskKeyRef) -> crate::Result<ConnectionRef> {
        let address = dask_key_ref_to_string(address);
        let address = address.trim_start_matches("tcp://");
        let stream = TcpStream::connect(&address).await?;
        Ok(ConnectionRef::wrap(stream))
    }
}
