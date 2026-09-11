use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
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
    #[inline(always)]
    fn code_and_payload(&self) -> (u8, &[u8]) {
        match self {
            Response::Ok(p) => (0, p),
            Response::Pong(p) => (1, p),
            Response::KeyValue(p) => (2, p),
            Response::KeyNotFound(p) => (3, p),
            Response::InvalidRequest(p) => (4, p),
            Response::PayloadTooLarge(p) => (5, p),
            Response::InternalError(p) => (6, p),
        }
    }

    /// Send the length-prefixed response over the given TCP stream.
    pub async fn send_response(
        &self,
        socket: &mut TcpStream,
        write_buf: &mut BytesMut,
    ) -> Result<()> {
        let (code, payload) = self.code_and_payload();

        write_buf.clear();
        write_buf.reserve(1 + 4 + payload.len());
        write_buf.put_u8(code);
        write_buf.put_u32_le(payload.len() as u32);
        write_buf.put_slice(payload);

        // single syscall
        socket.write_all(write_buf).await?;
        Ok(())
    }

    /// Read a length-prefixed response from the given TCP stream.
    pub async fn read_response(socket: &mut TcpStream) -> Result<Self> {
        let status = socket.read_u8().await?;
        let len = socket.read_u32_le().await? as usize;
        let mut payload = BytesMut::zeroed(len); // unsafe set_len may escape uninit memory
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
