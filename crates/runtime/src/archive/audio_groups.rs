use super::{
    ArchiveError, ArchiveIndex, AudioPacket, AudioSegment, ReceiverTimeRange,
    audio::{MAX_AUDIO_BYTES, MAX_AUDIO_PACKETS},
};
use bytes::Bytes;
use liazo_core::protocol::{AudioMeta, SourceKey};
use std::collections::HashMap;
use uuid::Uuid;

pub(super) struct Group {
    pub segment: AudioSegment,
    chunks: Vec<Bytes>,
    bytes: usize,
}

impl Group {
    pub fn write(self, index: &mut ArchiveIndex) -> Result<(), ArchiveError> {
        let mut bytes = Vec::with_capacity(self.bytes);
        for chunk in self.chunks {
            bytes.extend_from_slice(&chunk);
        }
        index.record_audio(self.segment, &bytes)
    }
}
#[derive(Default)]
pub(super) struct AudioGroups {
    groups: HashMap<SourceKey, Group>,
}
impl AudioGroups {
    pub fn push(
        &mut self,
        metadata: AudioMeta,
        bytes: Bytes,
        received_ns: u64,
        range: Option<ReceiverTimeRange>,
    ) -> Result<Vec<Group>, ArchiveError> {
        metadata
            .validate_payload(bytes.len())
            .map_err(|_| ArchiveError::InvalidPayload)?;
        let mut ready = vec![];
        let rollover = self.groups.get(&metadata.source).is_some_and(|group| {
            let last = group.segment.packets.last().unwrap();
            metadata.discontinuity
                || group.segment.stream_id != metadata.stream_id
                || group.segment.sample_rate != metadata.sample_rate
                || group.segment.channels != metadata.channels
                || last.sequence.checked_add(1) != Some(metadata.sequence)
                || last.sample_position + u64::from(last.frames) != metadata.sample_position
                || last.captured_ns > metadata.captured_ns
                || last.received_ns > received_ns
                || group.bytes + bytes.len() > MAX_AUDIO_BYTES
                || group.segment.packets.len() == MAX_AUDIO_PACKETS
        });
        if rollover {
            ready.push(self.groups.remove(&metadata.source).unwrap());
        }
        if !self.groups.contains_key(&metadata.source) && self.groups.len() == 8 {
            return Err(ArchiveError::Capacity);
        }
        let group = self
            .groups
            .entry(metadata.source.clone())
            .or_insert_with(|| Group {
                segment: AudioSegment {
                    id: Uuid::new_v4(),
                    source: metadata.source.clone(),
                    stream_id: metadata.stream_id,
                    sample_rate: metadata.sample_rate,
                    channels: metadata.channels,
                    packets: vec![],
                },
                chunks: vec![],
                bytes: 0,
            });
        group.segment.packets.push(AudioPacket {
            sequence: metadata.sequence,
            sample_position: metadata.sample_position,
            frames: metadata.frames,
            captured_ns: metadata.captured_ns,
            received_ns,
            receiver_range: range,
            discontinuity: metadata.discontinuity,
        });
        group.bytes += bytes.len();
        group.chunks.push(bytes);
        let first = group.segment.packets.first().unwrap();
        if metadata.sample_position + u64::from(metadata.frames) - first.sample_position
            >= 2 * u64::from(metadata.sample_rate)
            || group.segment.packets.len() == MAX_AUDIO_PACKETS
            || group.bytes == MAX_AUDIO_BYTES
        {
            ready.push(self.groups.remove(&metadata.source).unwrap());
        }
        Ok(ready)
    }
    pub fn take(&mut self, now: u64, all: bool) -> Vec<Group> {
        let due: Vec<_> = self
            .groups
            .iter()
            .filter(|(_, group)| {
                all || now.saturating_sub(group.segment.packets[0].received_ns) >= 2_000_000_000
            })
            .map(|(source, _)| source.clone())
            .collect();
        due.into_iter()
            .map(|source| self.groups.remove(&source).unwrap())
            .collect()
    }
    pub fn bytes(&self) -> u64 {
        self.groups
            .values()
            .map(|group| group.bytes as u64 + group.segment.packets.len() as u64 * 256 + 2048)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stream_changes_and_bounded_packet_counts_cannot_create_a_false_contiguous_clip() {
        let source = SourceKey {
            id: "audio:fixture".into(),
            generation: Uuid::new_v4(),
        };
        let mut metadata = AudioMeta {
            source,
            stream_id: Uuid::new_v4(),
            sequence: 1,
            sample_position: 0,
            sample_rate: 16000,
            channels: 1,
            frames: 1,
            captured_ns: 0,
            discontinuity: false,
        };
        let mut groups = AudioGroups::default();
        for index in 0..128 {
            metadata.sequence = index + 1;
            metadata.sample_position = index;
            metadata.captured_ns = index * 62500;
            let ready = groups
                .push(metadata.clone(), Bytes::from_static(&[0; 4]), index, None)
                .unwrap();
            assert_eq!(ready.len(), usize::from(index == 127));
            if index == 127 {
                assert_eq!(ready[0].segment.packets.len(), 128);
            }
        }
        assert_eq!(groups.bytes(), 0);
        metadata.sequence = 129;
        metadata.sample_position = 128;
        assert!(
            groups
                .push(metadata.clone(), Bytes::from_static(&[0; 4]), 129, None)
                .unwrap()
                .is_empty()
        );
        metadata.stream_id = Uuid::new_v4();
        metadata.sequence = 1;
        metadata.sample_position = 0;
        let ready = groups
            .push(metadata, Bytes::from_static(&[0; 4]), 130, None)
            .unwrap();
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].segment.packets[0].sequence, 129);
        assert_eq!(groups.take(131, true)[0].segment.packets[0].sequence, 1);
    }
}
