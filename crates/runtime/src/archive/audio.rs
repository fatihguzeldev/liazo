use super::{ArchiveError, ArchiveIndex, PayloadDescriptor, ReceiverTimeRange};
use bytes::Bytes;
use liazo_core::protocol::{AudioMeta, SourceKey};
use rusqlite::{OptionalExtension, params};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub(crate) const MAX_AUDIO_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_AUDIO_PACKETS: usize = 128;
const MAX_PACKET_JSON: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AudioPacket {
    pub sequence: u64,
    pub sample_position: u64,
    pub frames: u32,
    pub captured_ns: u64,
    pub received_ns: u64,
    pub receiver_range: Option<ReceiverTimeRange>,
    pub discontinuity: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AudioSegment {
    pub id: Uuid,
    pub source: SourceKey,
    pub stream_id: Uuid,
    pub sample_rate: u32,
    pub channels: u8,
    pub packets: Vec<AudioPacket>,
}

#[derive(Clone, Debug, Serialize, schemars::JsonSchema)]
pub struct AudioClip {
    pub segment: AudioSegment,
    #[serde(skip)]
    pub wav: Bytes,
}

#[derive(Clone, Debug, Serialize, schemars::JsonSchema)]
pub struct AudioPolicyInterval {
    pub source: SourceKey,
    pub first_ns: u64,
    pub end_ns: Option<u64>,
}
#[derive(Clone, Debug, Serialize, schemars::JsonSchema)]
pub struct AudioGap {
    pub source: Option<SourceKey>,
    pub first_ns: u64,
    pub end_ns: Option<u64>,
    pub reason: crate::history::GapReason,
}
#[derive(Clone, Debug, Serialize, schemars::JsonSchema)]
pub struct AudioPage {
    pub first_ns: u64,
    pub last_ns: u64,
    pub clips: Vec<AudioClip>,
    pub policy: Vec<AudioPolicyInterval>,
    pub gaps: Vec<AudioGap>,
    pub truncated: bool,
    pub retention_floor_ns: u64,
    pub capacity_before_ns: Option<u64>,
}

impl AudioSegment {
    pub(crate) fn validate(&self, bytes: &[u8]) -> Result<(), ArchiveError> {
        if self.id.is_nil()
            || self.packets.is_empty()
            || self.packets.len() > MAX_AUDIO_PACKETS
            || bytes.len() > MAX_AUDIO_BYTES
        {
            return Err(ArchiveError::InvalidPayload);
        }
        let mut offset = 0usize;
        let mut previous: Option<&AudioPacket> = None;
        for packet in &self.packets {
            let length = packet.frames as usize * usize::from(self.channels) * 4;
            AudioMeta {
                source: self.source.clone(),
                stream_id: self.stream_id,
                sequence: packet.sequence,
                sample_position: packet.sample_position,
                frames: packet.frames,
                captured_ns: packet.captured_ns,
                sample_rate: self.sample_rate,
                channels: self.channels,
                discontinuity: packet.discontinuity,
            }
            .validate_payload(length)
            .map_err(|_| ArchiveError::InvalidPayload)?;
            if packet.received_ns > i64::MAX as u64
                || packet.receiver_range.is_some_and(|range| {
                    range.earliest_ns > range.latest_ns || range.latest_ns > i64::MAX as u64
                })
            {
                return Err(ArchiveError::InvalidPayload);
            }
            if previous.is_some_and(|old| {
                packet.discontinuity
                    || old.sequence.checked_add(1) != Some(packet.sequence)
                    || old.sample_position + u64::from(old.frames) != packet.sample_position
                    || old.captured_ns > packet.captured_ns
                    || old.received_ns > packet.received_ns
            }) {
                return Err(ArchiveError::InvalidPayload);
            }
            offset = offset
                .checked_add(length)
                .ok_or(ArchiveError::InvalidPayload)?;
            previous = Some(packet);
        }
        if offset != bytes.len()
            || bytes
                .chunks_exact(4)
                .any(|sample| !f32::from_le_bytes(sample.try_into().unwrap()).is_finite())
        {
            return Err(ArchiveError::InvalidPayload);
        }
        Ok(())
    }
    pub fn receiver_range(&self) -> ReceiverTimeRange {
        ReceiverTimeRange {
            earliest_ns: self
                .packets
                .iter()
                .map(|packet| {
                    packet
                        .receiver_range
                        .map_or(packet.received_ns, |range| range.earliest_ns)
                })
                .min()
                .unwrap_or(0),
            latest_ns: self
                .packets
                .iter()
                .map(|packet| {
                    packet
                        .receiver_range
                        .map_or(packet.received_ns, |range| range.latest_ns)
                })
                .max()
                .unwrap_or(0),
        }
    }
    pub(crate) fn last_received_ns(&self) -> u64 {
        self.packets.last().map_or(0, |packet| packet.received_ns)
    }
}

impl ArchiveIndex {
    pub(super) fn initialize_audio(&self) -> Result<(), ArchiveError> {
        self.database.execute_batch("CREATE TABLE IF NOT EXISTS audio (id TEXT PRIMARY KEY, source TEXT NOT NULL,
            generation TEXT NOT NULL, payload TEXT NOT NULL REFERENCES payloads(id),
            first_ns INTEGER NOT NULL, last_ns INTEGER NOT NULL, received_ns INTEGER NOT NULL);
            CREATE INDEX IF NOT EXISTS audio_time ON audio(source,generation,first_ns,last_ns);
            CREATE INDEX IF NOT EXISTS audio_retention ON audio(last_ns);
            CREATE TABLE IF NOT EXISTS audio_policy (id INTEGER PRIMARY KEY, source TEXT NOT NULL, generation TEXT NOT NULL,
            first_ns INTEGER NOT NULL, end_ns INTEGER);
            CREATE UNIQUE INDEX IF NOT EXISTS audio_policy_open ON audio_policy(source,generation) WHERE end_ns IS NULL;
            CREATE INDEX IF NOT EXISTS audio_policy_time ON audio_policy(first_ns,end_ns);")?;
        Ok(())
    }
    pub fn record_audio_policy(
        &mut self,
        selected: &[SourceKey],
        at: u64,
    ) -> Result<(), ArchiveError> {
        if at > i64::MAX as u64
            || selected.len() > 8
            || selected.iter().any(|source| source.validate().is_err())
            || selected
                .iter()
                .collect::<std::collections::HashSet<_>>()
                .len()
                != selected.len()
        {
            return Err(ArchiveError::InvalidPayload);
        }
        self.make_room(at, 0)?;
        let latest: Option<u64> = self.database.query_row(
            "SELECT MAX(COALESCE(end_ns,first_ns)) FROM audio_policy",
            [],
            |row| row.get(0),
        )?;
        if latest.is_some_and(|latest| latest > at) {
            return Err(ArchiveError::InvalidPayload);
        }
        let open = self
            .database
            .prepare("SELECT source,generation FROM audio_policy WHERE end_ns IS NULL")?
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let tx = self.database.transaction()?;
        for (source, generation) in &open {
            if !selected
                .iter()
                .any(|key| key.id == *source && key.generation.to_string() == *generation)
            {
                tx.execute("UPDATE audio_policy SET end_ns=?3 WHERE source=?1 AND generation=?2 AND end_ns IS NULL", params![source, generation, at])?;
            }
        }
        for source in selected {
            tx.execute(
                "INSERT OR IGNORE INTO audio_policy (source,generation,first_ns) VALUES (?1,?2,?3)",
                params![source.id, source.generation.to_string(), at],
            )?;
        }
        tx.commit()?;
        Ok(())
    }
    pub fn record_audio(
        &mut self,
        segment: AudioSegment,
        bytes: &[u8],
    ) -> Result<(), ArchiveError> {
        segment.validate(bytes)?;
        let json = serde_json::to_vec(&segment)?;
        if json.len() > MAX_PACKET_JSON {
            return Err(ArchiveError::InvalidPayload);
        }
        let mut encoded = Vec::with_capacity(8 + json.len() + bytes.len());
        encoded.extend_from_slice(b"LZA1");
        encoded.extend_from_slice(&(json.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&json);
        encoded.extend_from_slice(bytes);
        let reserve = (encoded.len() as u64).div_ceil(4096) * 4096 + 4096;
        let id = segment.id.to_string();
        if self.database.query_row(
            "SELECT EXISTS(SELECT 1 FROM payloads WHERE id=?1)",
            [&id],
            |row| row.get::<_, bool>(0),
        )? {
            return Err(ArchiveError::IdentityCollision);
        }
        self.make_room(segment.last_received_ns(), reserve)?;
        self.database.execute(
            "INSERT INTO payloads (id,length,hash) VALUES (?1,?2,?3)",
            params![id, encoded.len() as u64, [0u8; 32].as_slice()],
        )?;
        let descriptor = self.payloads.put(segment.id, &encoded)?;
        let range = segment.receiver_range();
        let tx = self.database.transaction()?;
        tx.execute("UPDATE gaps SET end_ns=?3 WHERE source=?1 AND generation=?2 AND end_ns IS NULL AND first_ns<=?3", params![segment.source.id, segment.source.generation.to_string(), segment.packets[0].received_ns])?;
        tx.execute(
            "UPDATE payloads SET hash=?2 WHERE id=?1",
            params![id, descriptor.sha256.as_slice()],
        )?;
        tx.execute("INSERT INTO audio (id,source,generation,payload,first_ns,last_ns,received_ns) VALUES (?1,?2,?3,?1,?4,?5,?6)", params![id, segment.source.id, segment.source.generation.to_string(), range.earliest_ns, range.latest_ns, segment.last_received_ns()])?;
        tx.commit()?;
        Ok(())
    }
    pub fn audio_between(
        &self,
        first: u64,
        last: u64,
        selected: &[SourceKey],
    ) -> Result<AudioPage, ArchiveError> {
        if first > last
            || last > i64::MAX as u64
            || selected.len() > 8
            || selected.iter().any(|source| source.validate().is_err())
        {
            return Err(ArchiveError::InvalidPayload);
        }
        let floor = self.stats()?.retention_floor_ns;
        let capacity: Option<String> = self
            .database
            .query_row(
                "SELECT value FROM meta WHERE key='capacity_before'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        let mut page = AudioPage {
            first_ns: first,
            last_ns: last,
            clips: vec![],
            policy: vec![],
            gaps: vec![],
            truncated: false,
            retention_floor_ns: floor,
            capacity_before_ns: capacity
                .map(|value| value.parse())
                .transpose()
                .map_err(|_| ArchiveError::InvalidPayload)?,
        };
        if last < floor {
            return Ok(page);
        }
        let mut query = self.database.prepare("SELECT source,generation,payload,length,hash FROM audio JOIN payloads ON payloads.id=audio.payload WHERE audio.last_ns>=?1 AND audio.first_ns<=?2 ORDER BY audio.first_ns,audio.id")?;
        let mut rows = query.query(params![first.max(floor), last])?;
        let mut budget = 16 * 1024 * 1024;
        while let Some(row) = rows.next()? {
            let source = key(row.get(0)?, row.get(1)?)?;
            if !selected.is_empty() && !selected.contains(&source) {
                continue;
            }
            let length: u64 = row.get(3)?;
            if page.clips.len() == 32 || length > budget {
                page.truncated = true;
                break;
            }
            budget -= length;
            let descriptor = PayloadDescriptor {
                id: row
                    .get::<_, String>(2)?
                    .parse()
                    .map_err(|_| ArchiveError::InvalidPayload)?,
                length,
                sha256: row
                    .get::<_, Vec<u8>>(4)?
                    .try_into()
                    .map_err(|_| ArchiveError::CorruptPayload)?,
            };
            let bytes = self.payloads.read(&descriptor)?;
            if bytes.len() < 8 || &bytes[..4] != b"LZA1" {
                return Err(ArchiveError::CorruptPayload);
            }
            let size = u32::from_le_bytes(bytes[4..8].try_into().unwrap()) as usize;
            if size > MAX_PACKET_JSON || size + 8 > bytes.len() {
                return Err(ArchiveError::CorruptPayload);
            }
            let segment: AudioSegment = serde_json::from_slice(&bytes[8..8 + size])?;
            let pcm = &bytes[8 + size..];
            segment.validate(pcm)?;
            if segment.id != descriptor.id || segment.source != source {
                return Err(ArchiveError::CorruptPayload);
            }
            page.clips.push(AudioClip {
                wav: wav(&segment, pcm),
                segment,
            });
        }
        let mut policy_query = self.database.prepare("SELECT source,generation,first_ns,end_ns FROM audio_policy WHERE (end_ns IS NULL OR end_ns>=?1) AND first_ns<=?2 ORDER BY first_ns,id")?;
        let mut rows = policy_query.query(params![first.max(floor), last])?;
        while let Some(row) = rows.next()? {
            let source = key(row.get(0)?, row.get(1)?)?;
            if !selected.is_empty() && !selected.contains(&source) {
                continue;
            }
            if page.policy.len() == 256 {
                page.truncated = true;
                break;
            }
            page.policy.push(AudioPolicyInterval {
                source,
                first_ns: row.get(2)?,
                end_ns: row.get(3)?,
            });
        }
        for global in [false, true] {
            let sql = if global {
                "SELECT NULL,NULL,first_ns,end_ns,reason FROM global_gaps WHERE (end_ns IS NULL OR end_ns>=?1) AND first_ns<=?2 ORDER BY first_ns,id"
            } else {
                "SELECT source,generation,first_ns,end_ns,reason FROM gaps WHERE (end_ns IS NULL OR end_ns>=?1) AND first_ns<=?2 AND EXISTS (SELECT 1 FROM audio_policy WHERE audio_policy.source=gaps.source AND audio_policy.generation=gaps.generation) ORDER BY first_ns,id"
            };
            let mut query = self.database.prepare(sql)?;
            let mut rows = query.query(params![first.max(floor), last])?;
            while let Some(row) = rows.next()? {
                let source = if global {
                    None
                } else {
                    Some(key(row.get(0)?, row.get(1)?)?)
                };
                if source
                    .as_ref()
                    .is_some_and(|source| !selected.is_empty() && !selected.contains(source))
                {
                    continue;
                }
                if page.gaps.len() == 256 {
                    page.truncated = true;
                    break;
                }
                page.gaps.push(AudioGap {
                    source,
                    first_ns: row.get(2)?,
                    end_ns: row.get(3)?,
                    reason: serde_json::from_str(&row.get::<_, String>(4)?)?,
                });
            }
        }
        Ok(page)
    }
}
fn key(id: String, generation: String) -> Result<SourceKey, ArchiveError> {
    let source = SourceKey {
        id,
        generation: generation
            .parse()
            .map_err(|_| ArchiveError::InvalidPayload)?,
    };
    source
        .validate()
        .map_err(|_| ArchiveError::InvalidPayload)?;
    Ok(source)
}
fn wav(segment: &AudioSegment, pcm: &[u8]) -> Bytes {
    let length = pcm.len() as u32;
    let align = u16::from(segment.channels) * 4;
    let frames = length / u32::from(align);
    let mut bytes = Vec::with_capacity(pcm.len() + 58);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(length + 50).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&18u32.to_le_bytes());
    bytes.extend_from_slice(&3u16.to_le_bytes());
    bytes.extend_from_slice(&u16::from(segment.channels).to_le_bytes());
    bytes.extend_from_slice(&segment.sample_rate.to_le_bytes());
    bytes.extend_from_slice(&(segment.sample_rate * u32::from(align)).to_le_bytes());
    bytes.extend_from_slice(&align.to_le_bytes());
    bytes.extend_from_slice(&32u16.to_le_bytes());
    bytes.extend_from_slice(&0u16.to_le_bytes());
    bytes.extend_from_slice(b"fact");
    bytes.extend_from_slice(&4u32.to_le_bytes());
    bytes.extend_from_slice(&frames.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&length.to_le_bytes());
    bytes.extend_from_slice(pcm);
    bytes.into()
}
