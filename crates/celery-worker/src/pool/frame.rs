//! Binary master-child framing: length, type, job ID and opaque payload. The
//! supervisor never decodes task bodies or pickled result values.

use std::io::{self, Read};

use bytes::{BufMut, Bytes, BytesMut};

/// Fixed type and job ID length.
pub const META_LEN: usize = 1 + 8;

/// Frame length prefix size.
pub const LEN_LEN: usize = 4;

/// Bounds allocation when a corrupt header claims an arbitrary frame size.
pub const MAX_FRAME: usize = 256 * 1024 * 1024;

/// Frame type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameKind {
    /// Job from master to child.
    Task,
    /// Acceptance from child with its monotonic timestamp as f64.
    Ack,
    /// Result from child with a success byte and pickled value.
    Ready,
    /// Request to discard backend sockets after broker restart.
    ResetBackend,
}

impl FrameKind {
    fn as_u8(self) -> u8 {
        match self {
            Self::Task => 1,
            Self::Ack => 2,
            Self::Ready => 3,
            Self::ResetBackend => 4,
        }
    }

    fn from_u8(value: u8) -> io::Result<Self> {
        match value {
            1 => Ok(Self::Task),
            2 => Ok(Self::Ack),
            3 => Ok(Self::Ready),
            4 => Ok(Self::ResetBackend),
            other => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown frame type: {other}"),
            )),
        }
    }
}

/// Complete frame with its payload still serialized.
#[derive(Debug, Clone)]
pub struct Frame {
    pub kind: FrameKind,
    pub job: u64,
    pub payload: Bytes,
}

impl Frame {
    pub fn new(kind: FrameKind, job: u64, payload: Bytes) -> Self {
        Self { kind, job, payload }
    }

    /// Returns header and payload slices for vectored writes.
    pub fn header(&self) -> [u8; LEN_LEN + META_LEN] {
        let total = META_LEN + self.payload.len();
        let mut header = [0u8; LEN_LEN + META_LEN];
        header[..4].copy_from_slice(&(total as u32).to_be_bytes());
        header[4] = self.kind.as_u8();
        header[5..13].copy_from_slice(&self.job.to_be_bytes());
        header
    }

    /// Serializes the frame for tests and zygote control.
    pub fn encode(&self) -> BytesMut {
        let header = self.header();
        let mut out = BytesMut::with_capacity(header.len() + self.payload.len());
        out.put_slice(&header);
        out.put_slice(&self.payload);
        out
    }
}

/// Reads a frame, returning None for clean EOF at a frame boundary.
pub fn read_frame(reader: &mut impl Read) -> io::Result<Option<Frame>> {
    let mut len = [0u8; LEN_LEN];
    let mut read = 0;
    while read < LEN_LEN {
        match reader.read(&mut len[read..])? {
            0 if read == 0 => return Ok(None),
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "eof in frame header",
                ));
            }
            n => read += n,
        }
    }
    let length = u32::from_be_bytes(len) as usize;
    if length < META_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too short: {length}"),
        ));
    }
    if length > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame too large: {length}"),
        ));
    }
    let mut body = vec![0u8; length];
    reader.read_exact(&mut body)?;
    let kind = FrameKind::from_u8(body[0])?;
    let mut job = [0u8; 8];
    job.copy_from_slice(&body[1..9]);
    Ok(Some(Frame {
        kind,
        job: u64::from_be_bytes(job),
        payload: Bytes::from(body).slice(META_LEN..),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn roundtrip() {
        let frame = Frame::new(FrameKind::Task, 42, Bytes::from_static(b"ciao"));
        let encoded = frame.encode();
        let mut cursor = Cursor::new(&encoded[..]);
        let decoded = read_frame(&mut cursor).expect("io").expect("frame");
        assert_eq!(decoded.kind, FrameKind::Task);
        assert_eq!(decoded.job, 42);
        assert_eq!(&decoded.payload[..], b"ciao");
        assert!(read_frame(&mut cursor).expect("io").is_none());
    }

    #[test]
    fn reset_backend_roundtrip() {
        let frame = Frame::new(
            FrameKind::ResetBackend,
            0,
            Bytes::from_static(b"redis://host/1"),
        );
        let encoded = frame.encode();
        let decoded = read_frame(&mut Cursor::new(&encoded[..]))
            .expect("io")
            .expect("frame");
        assert_eq!(decoded.kind, FrameKind::ResetBackend);
        assert_eq!(decoded.job, 0);
        assert_eq!(decoded.payload, frame.payload);
    }

    #[test]
    fn large_payload_survives_following_frames() {
        let payload = Bytes::from(vec![0xa5; 1024 * 1024]);
        let mut encoded = Frame::new(FrameKind::Ready, 42, payload.clone()).encode();
        encoded.extend_from_slice(&Frame::new(FrameKind::Ack, 43, Bytes::new()).encode());
        let mut cursor = Cursor::new(&encoded[..]);
        let first = read_frame(&mut cursor).expect("io").expect("frame");
        let second = read_frame(&mut cursor).expect("io").expect("frame");
        assert_eq!(first.payload, payload);
        assert_eq!(first.job, 42);
        assert_eq!(second.job, 43);
        assert!(second.payload.is_empty());
        assert!(read_frame(&mut cursor).expect("io").is_none());
    }

    #[test]
    fn empty_payload() {
        let frame = Frame::new(FrameKind::Ack, 1, Bytes::new());
        let encoded = frame.encode();
        let decoded = read_frame(&mut Cursor::new(&encoded[..]))
            .expect("io")
            .expect("frame");
        assert_eq!(decoded.kind, FrameKind::Ack);
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn rejects_short_and_huge_frames() {
        let short = (8u32).to_be_bytes();
        let err = read_frame(&mut Cursor::new(short)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let huge = ((MAX_FRAME + 1) as u32).to_be_bytes();
        let err = read_frame(&mut Cursor::new(huge)).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn rejects_truncated_body() {
        let frame = Frame::new(FrameKind::Ready, 7, Bytes::from_static(b"0123456789"));
        let encoded = frame.encode();
        let mut cursor = Cursor::new(&encoded[..encoded.len() - 3]);
        let err = read_frame(&mut cursor).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn rejects_unknown_kind() {
        let mut bytes = Frame::new(FrameKind::Task, 1, Bytes::new()).encode();
        bytes[4] = 99;
        let err = read_frame(&mut Cursor::new(&bytes[..])).unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
