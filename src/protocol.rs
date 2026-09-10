use anyhow::Result;
use bytes::{Bytes, BytesMut};
use std::io::IoSlice;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Represents the different operations that clients can perform in the mini-db protocol.
pub enum Operation {
    Get,
    Set,
    Delete,
    Ping,
    Close,
}

impl Operation {
    #[inline(always)]
    pub fn from_u8(value: u8) -> Option<Self> {
        match value {
            0 => Some(Operation::Get),
            1 => Some(Operation::Set),
            2 => Some(Operation::Delete),
            3 => Some(Operation::Ping),
            4 => Some(Operation::Close),
            _ => None,
        }
    }
    #[inline(always)]
    pub fn to_u8(&self) -> u8 {
        match self {
            Operation::Get => 0,
            Operation::Set => 1,
            Operation::Delete => 2,
            Operation::Ping => 3,
            Operation::Close => 4,
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Ok(Bytes),
    Pong(Bytes),
    KeyValue(Bytes),
    KeyNotFound(Bytes),
    InvalidRequest(Bytes),
    PayloadTooLarge(Bytes),
    InternalError(Bytes),
}

impl Response {
    /// Send the length-prefixed response over the given TCP stream.
    pub async fn send_response(&self, socket: &mut TcpStream) -> Result<()> {
        match self {
            Response::Ok(payload) => {
                let mut header = [0u8; 5];
                header[0] = 0;
                header[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());

                let bufs = [IoSlice::new(&header), IoSlice::new(payload)];
                let _ = socket.write_vectored(&bufs).await?;
            }
            Response::Pong(payload) => {
                let mut header = [0u8; 5];
                header[0] = 1;
                header[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());

                let bufs = [IoSlice::new(&header), IoSlice::new(payload)];
                let _ = socket.write_vectored(&bufs).await?;
            }
            Response::KeyValue(payload) => {
                let mut header = [0u8; 5];
                header[0] = 2;
                header[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());

                let bufs = [IoSlice::new(&header), IoSlice::new(payload)];
                let _ = socket.write_vectored(&bufs).await?;
            }
            Response::KeyNotFound(payload) => {
                let mut header = [0u8; 5];
                header[0] = 3;
                header[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());

                let bufs = [IoSlice::new(&header), IoSlice::new(payload)];
                let _ = socket.write_vectored(&bufs).await?;
            }
            Response::InvalidRequest(payload) => {
                let mut header = [0u8; 5];
                header[0] = 4;
                header[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());

                let bufs = [IoSlice::new(&header), IoSlice::new(payload)];
                let _ = socket.write_vectored(&bufs).await?;
            }
            Response::PayloadTooLarge(payload) => {
                let mut header = [0u8; 5];
                header[0] = 5;
                header[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());

                let bufs = [IoSlice::new(&header), IoSlice::new(payload)];
                let _ = socket.write_vectored(&bufs).await?;
            }
            Response::InternalError(payload) => {
                let mut header = [0u8; 5];
                header[0] = 6;
                header[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());

                let bufs = [IoSlice::new(&header), IoSlice::new(payload)];
                let _ = socket.write_vectored(&bufs).await?;
            }
        }
        Ok(())
    }

    /// Read a length-prefixed response from the given TCP stream.
    pub async fn read_response(socket: &mut TcpStream) -> Result<Self> {
        let status = socket.read_u8().await?;
        let len = socket.read_u32_le().await? as usize;
        let mut payload = BytesMut::zeroed(len);
        socket.read_exact(&mut payload).await?;

        let payload = payload.freeze(); // convert to immutable Bytes
        match status {
            0 => Ok(Response::Ok(payload)),
            1 => Ok(Response::Pong(payload)),
            2 => Ok(Response::KeyValue(payload)),
            3 => Ok(Response::KeyNotFound(payload)),
            4 => Ok(Response::InvalidRequest(payload)),
            5 => Ok(Response::PayloadTooLarge(payload)),
            6 => Ok(Response::InternalError(payload)),
            _ => Ok(Response::InternalError(Bytes::from(format!(
                "Unknown response code: {}",
                status
            )))),
        }
    }
}
