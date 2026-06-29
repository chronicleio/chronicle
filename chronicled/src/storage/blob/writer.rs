use std::fs::File;

use chronicle_proto::pb_ext::Event;
use prost::Message;

use crate::error::unit_error::UnitError;
use crate::segment::Segment;

use super::ENTRY_HEADER_SIZE;
use super::copy::copy_range;

pub struct BlobWriter {
    segment: Box<dyn Segment>,
    segment_id: u64,
    entry_count: u64,
    size: u64,
}

impl BlobWriter {
    pub fn new(segment: Box<dyn Segment>, segment_id: u64) -> Self {
        Self {
            segment,
            segment_id,
            entry_count: 0,
            size: 0,
        }
    }

    pub async fn write_raw(&mut self, raw: &[u8]) -> Result<(u64, u32), UnitError> {
        let total_len = raw.len() as u32;
        let entry_start = self
            .segment
            .write(raw)
            .await
            .map_err(|e| UnitError::Storage(e.to_string()))?;

        self.entry_count += 1;
        self.size += total_len as u64;

        Ok((entry_start, total_len))
    }

    pub fn write_range_from(
        &mut self,
        source: &File,
        src_offset: u64,
        length: u64,
        entry_count: u64,
    ) -> Result<u64, UnitError> {
        let dst_offset = self.size;

        if let Some(dst_file) = self.segment.as_std_file() {
            copy_range(source, src_offset, dst_file, dst_offset, length)
                .map_err(|e| UnitError::Storage(format!("copy_range failed: {}", e)))?;
        } else {
            return Err(UnitError::Storage(
                "write_range_from requires a file-backed segment".into(),
            ));
        }

        self.segment.advance_offset(length);
        self.size += length;
        self.entry_count += entry_count;

        Ok(dst_offset)
    }

    pub async fn write_entry(&mut self, event: &Event) -> Result<(u64, u32), UnitError> {
        let proto_data = event.encode_to_vec();
        let payload_len = proto_data.len() as u32;
        let total_len = ENTRY_HEADER_SIZE as u32 + payload_len;

        let mut buf = Vec::with_capacity(total_len as usize);
        buf.extend_from_slice(&event.timeline_id.to_be_bytes());
        buf.extend_from_slice(&event.offset.to_be_bytes());
        buf.extend_from_slice(&payload_len.to_le_bytes());
        buf.extend_from_slice(&proto_data);

        let entry_start = self
            .segment
            .write(&buf)
            .await
            .map_err(|e| UnitError::Storage(e.to_string()))?;

        self.entry_count += 1;
        self.size += total_len as u64;

        Ok((entry_start, total_len))
    }

    pub async fn finish(self) -> Result<(), UnitError> {
        self.segment
            .sync()
            .await
            .map_err(|e| UnitError::Storage(format!("failed to fsync segment file: {}", e)))?;
        Ok(())
    }

    pub fn segment_id(&self) -> u64 {
        self.segment_id
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn size(&self) -> u64 {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::segment::standard::StandardSegment;
    use crate::storage::blob::BlobReader;
    use std::path::Path;

    async fn make_writer(path: &Path, segment_id: u64) -> BlobWriter {
        let seg = StandardSegment::new(path.to_path_buf()).await.unwrap();
        BlobWriter::new(Box::new(seg), segment_id)
    }

    #[tokio::test]
    async fn test_segment_write_and_read() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment_000001.cseg");

        let event = Event {
            timeline_id: 1,
            term: 1,
            offset: 0,
            payload: Some(b"hello".to_vec().into()),
            crc32: None,
            timestamp: 100,
            schema_id: 0,
        };

        let (byte_offset, length) = {
            let mut writer = make_writer(&path, 1).await;
            let result = writer.write_entry(&event).await.unwrap();
            assert_eq!(writer.entry_count(), 1);
            writer.finish().await.unwrap();
            result
        };

        let reader = BlobReader::open(&path).unwrap();
        let read_event = reader.read_event(byte_offset, length).unwrap();
        assert_eq!(read_event.timeline_id, 1);
        assert_eq!(read_event.offset, 0);
        assert_eq!(read_event.payload, Some(b"hello".to_vec().into()));
        assert_eq!(read_event.timestamp, 100);
    }

    #[tokio::test]
    async fn test_segment_multiple_entries() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("segment_000002.cseg");

        let mut entries = Vec::new();
        {
            let mut writer = make_writer(&path, 2).await;
            for i in 0..10 {
                let event = Event {
                    timeline_id: 1,
                    term: 1,
                    offset: i,
                    payload: Some(format!("event_{}", i).into_bytes().into()),
                    crc32: None,
                    timestamp: i * 100,
                    schema_id: 0,
                };
                let (offset, len) = writer.write_entry(&event).await.unwrap();
                entries.push((offset, len, i));
            }
            assert_eq!(writer.entry_count(), 10);
            writer.finish().await.unwrap();
        }

        let reader = BlobReader::open(&path).unwrap();
        for (byte_offset, length, expected_offset) in entries {
            let event = reader.read_event(byte_offset, length).unwrap();
            assert_eq!(event.offset, expected_offset);
            assert_eq!(event.timeline_id, 1);
        }
    }
}
