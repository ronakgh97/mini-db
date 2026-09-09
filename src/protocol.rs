use anyhow::Result;
use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
                socket.write_u8(0).await?;
                socket.write_u32_le(payload.len() as u32).await?;
                socket.write_all(payload).await?;
            }
            Response::Pong(payload) => {
                socket.write_u8(1).await?;
                socket.write_u32_le(payload.len() as u32).await?;
                socket.write_all(payload).await?;
            }
            Response::KeyValue(payload) => {
                socket.write_u8(2).await?;
                socket.write_u32_le(payload.len() as u32).await?;
                socket.write_all(payload).await?;
            }
            Response::KeyNotFound(payload) => {
                socket.write_u8(3).await?;
                socket.write_u32_le(payload.len() as u32).await?;
                socket.write_all(payload).await?;
            }
            Response::InvalidRequest(payload) => {
                socket.write_u8(4).await?;
                socket.write_u32_le(payload.len() as u32).await?;
                socket.write_all(payload).await?;
            }
            Response::PayloadTooLarge(payload) => {
                socket.write_u8(5).await?;
                socket.write_u32_le(payload.len() as u32).await?;
                socket.write_all(payload).await?;
            }
            Response::InternalError(payload) => {
                socket.write_u8(6).await?;
                socket.write_u32_le(payload.len() as u32).await?;
                socket.write_all(payload).await?;
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
