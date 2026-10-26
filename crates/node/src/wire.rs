//! The node wire format: length-prefixed JSON frames over TCP. A `u32`
//! big-endian length, then one [`Wire`] encoded as JSON.

use control::proto::Msg;
use control::runtime::{Addr, Endpoint};
use serde::{Deserialize, Serialize};
use std::io;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// The TCP port every node, the VIP and switch management interfaces listen on.
pub const PORT: u16 = 7400;
/// Frames larger than this are refused (a full sync of a big cluster is a few MB).
pub const MAX_FRAME: usize = 64 << 20;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Wire {
    /// The first frame the dialler sends: who it is, so the far end can
    /// answer it over this connection.
    Hello { from: Addr },
    /// Sent on an otherwise idle connection so the far end can tell it is
    /// still alive; a connection silent for too long is dropped.
    Keepalive,
    Msg { from: Endpoint, to: Endpoint, msg: Msg },
}

pub fn encode(w: &Wire) -> Vec<u8> {
    let body = serde_json::to_vec(w).expect("wire frames serialize");
    let mut out = Vec::with_capacity(4 + body.len());
    out.extend_from_slice(&u32::try_from(body.len()).expect("frame under 4 GB").to_be_bytes());
    out.extend_from_slice(&body);
    out
}

pub async fn write_frame(w: &mut (impl AsyncWrite + Unpin), frame: &Wire) -> io::Result<()> {
    w.write_all(&encode(frame)).await
}

pub async fn read_frame(r: &mut (impl AsyncRead + Unpin)) -> io::Result<Wire> {
    let len = r.read_u32().await? as usize;
    if len > MAX_FRAME {
        return Err(io::Error::new(io::ErrorKind::InvalidData, format!("frame of {len} bytes")));
    }
    let mut body = vec![0; len];
    r.read_exact(&mut body).await?;
    serde_json::from_slice(&body).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

#[cfg(test)]
mod tests {
    use super::*;
    use control::runtime::Svc;
    use model::HostId;

    #[tokio::test]
    async fn frames_round_trip() {
        let frames = vec![
            Wire::Hello { from: Addr::Host(HostId::from("a")) },
            Wire::Keepalive,
            Wire::Msg {
                from: Endpoint::new(Addr::Host(HostId::from("a")), Svc::Agent),
                to: Endpoint::new(Addr::Vip, Svc::Cp),
                msg: Msg::Ping(7),
            },
        ];
        let mut buf = vec![];
        for f in &frames {
            write_frame(&mut buf, f).await.unwrap();
        }
        let mut r = &buf[..];
        for f in &frames {
            assert_eq!(&read_frame(&mut r).await.unwrap(), f);
        }
        assert!(read_frame(&mut r).await.is_err(), "nothing left");
    }

    #[tokio::test]
    async fn oversized_frames_are_refused() {
        let buf = (MAX_FRAME as u32 + 1).to_be_bytes();
        assert_eq!(read_frame(&mut &buf[..]).await.unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
