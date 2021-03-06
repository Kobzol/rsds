use crate::prelude::*;
use tokio::prelude::*;

//use failure::Error;
use rmp_serde as rmps;

use crate::client::start_client;
use crate::daskcodec::DaskCodec;
use crate::messages::generic::{GenericMessage, IdentityResponse, SimpleMessage};
use crate::worker::start_worker;
use std::error::Error;
use tokio::codec::Framed;
use tokio::net::TcpStream;

pub async fn handle_connection(
    core_ref: CoreRef,
    socket: TcpStream,
    address: std::net::SocketAddr,
) {
    socket.set_nodelay(true).unwrap();
    let mut framed = Framed::new(socket, DaskCodec::new());
    /*
    let data = Bytes::from(&b"this should crash"[..]);
    framed.send(data).await.unwrap();

    println!("Send finished!");
    */
    log::debug!("New connection from {}", address);

    loop {
        let buffer = framed.next().await;
        match buffer {
            Some(data) => {
                let data = data.unwrap();
                let msg: Result<GenericMessage, _> = rmps::from_read(std::io::Cursor::new(&data));
                match msg {
                    Ok(GenericMessage::HeartbeatWorker(_)) => {
                        log::debug!("Heartbeat from worker");
                        continue;
                    }
                    Ok(GenericMessage::RegisterWorker(_m)) => {
                        log::debug!("Worker registration from {}", address);
                        start_worker(&core_ref, address, framed);
                        return;
                    }
                    Ok(GenericMessage::RegisterClient(m)) => {
                        log::debug!("Client registration from {}", address);
                        let rsp = SimpleMessage { op: "stream-start" };
                        let data = rmp_serde::encode::to_vec_named(&[rsp]).unwrap();
                        framed.send(data.into()).await.unwrap();
                        start_client(&core_ref, address, framed, m.client);
                        return;
                    }
                    Ok(GenericMessage::Identity(_)) => {
                        log::debug!("Identity request from {}", address);
                        let rsp = IdentityResponse {
                            i_type: "Scheduler",
                            id: core_ref.uid(),
                        };
                        let data = rmp_serde::encode::to_vec_named(&rsp).unwrap();
                        framed.send(data.into()).await.unwrap();
                    }
                    Err(e) => {
                        dbg!(data);
                        panic!(
                            "Invalid message from unregistered connection ({}): {}",
                            address,
                            e.description()
                        );
                    }
                }
            }
            None => {
                log::debug!("Unregistered connection {} closed", address);
                return;
            }
        }
    }
}
