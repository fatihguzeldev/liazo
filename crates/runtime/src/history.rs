use bytes::Bytes;
use liazo_core::{
    protocol::SourceKey,
    wire::{Frame, Header, ImageMeta},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, VecDeque},
    time::Duration,
};
use uuid::Uuid;

// Conservatively charge metadata, owned source strings, and collection slack.
const ROW_BYTES: u64 = 2048;
const PAYLOAD_OVERHEAD: u64 = 128;
const MAX_ROWS: usize = 8192;

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct HistoryLimits {
    pub duration_ms: u64,
    pub max_bytes: u64,
}
impl Default for HistoryLimits {
    fn default() -> Self {
        Self {
            duration_ms: 60_000,
            max_bytes: 64 * 1024 * 1024,
        }
    }
}
impl HistoryLimits {
    pub fn validate(self) -> Result<(), &'static str> {
        if !(100..=21_600_000).contains(&self.duration_ms)
            || !(4096..=2 * 1024 * 1024 * 1024).contains(&self.max_bytes)
        {
            return Err("invalid RAM history limits");
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GapReason {
    Disconnected,
    Stopped,
    PermissionRequired,
    Unavailable,
    NoData,
    SkippedFrames,
    Capacity,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "state", content = "reason", rename_all = "snake_case")]
pub enum HistoryState {
    Available,
    Sampled,
    Gap(GapReason),
    Expired,
    NotRetained,
    NotObserved,
}

#[derive(Clone, Serialize)]
pub struct HistoricalImage {
    pub source: SourceKey,
    pub state: HistoryState,
    pub first_observed_ns: Option<u64>,
    pub last_confirmed_ns: Option<u64>,
    pub first_capture: Option<ImageMeta>,
    pub last_capture: Option<ImageMeta>,
    #[serde(skip)]
    pub payload: Option<Bytes>,
}

#[derive(Clone, Serialize)]
pub struct HistorySnapshot {
    // Receiver monotonic session time. Capture metadata retains the provider clock.
    pub requested_ns: u64,
    pub now_ns: u64,
    pub retention_floor_ns: u64,
    pub images: Vec<HistoricalImage>,
    pub selection_truncated: bool,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct HistoryStats {
    pub accounted_bytes: u64,
    pub payloads: usize,
    pub intervals: usize,
    pub gaps: usize,
    pub evicted_intervals: u64,
}

struct Payload {
    bytes: Bytes,
    references: usize,
}
struct Interval {
    first_ns: u64,
    last_ns: u64,
    first: ImageMeta,
    last: ImageMeta,
    payload: Uuid,
    max_gap_ns: u64,
}
struct Gap {
    source: SourceKey,
    first_ns: u64,
    end_ns: Option<u64>,
    reason: GapReason,
}

pub struct RamHistory {
    limits: HistoryLimits,
    intervals: VecDeque<Interval>,
    gaps: VecDeque<Gap>,
    payloads: HashMap<Uuid, Payload>,
    bytes: u64,
    evicted: u64,
    capacity_before: Option<u64>,
    retention_floor: u64,
}

impl RamHistory {
    pub fn new(limits: HistoryLimits) -> Result<Self, &'static str> {
        limits.validate()?;
        Ok(Self {
            limits,
            intervals: VecDeque::new(),
            gaps: VecDeque::new(),
            payloads: HashMap::new(),
            bytes: 0,
            evicted: 0,
            capacity_before: None,
            retention_floor: 0,
        })
    }

    pub fn configure(&mut self, limits: HistoryLimits, now: u64) -> Result<(), &'static str> {
        limits.validate()?;
        self.limits = limits;
        self.trim(now);
        Ok(())
    }
    pub fn limits(&self) -> HistoryLimits {
        self.limits
    }

    pub fn image(
        &mut self,
        metadata: ImageMeta,
        id: Uuid,
        bytes: Bytes,
        arrived_ns: u64,
        max_gap: Duration,
    ) -> bool {
        if id.is_nil() || Frame::new(Header::Image(metadata.clone()), bytes.clone()).is_err() {
            return false;
        }
        self.trim(arrived_ns);
        if self
            .payloads
            .get(&id)
            .is_some_and(|payload| payload.bytes != bytes)
        {
            return false;
        }
        let source = &metadata.source;
        let prior = self
            .intervals
            .iter()
            .rposition(|row| row.last.source == *source);
        let had_gap = self
            .gaps
            .iter()
            .any(|gap| gap.source == *source && gap.end_ns.is_none());
        let mut interrupted = had_gap;
        if let Some(index) = prior {
            let row = &self.intervals[index];
            if arrived_ns < row.last_ns {
                return false;
            }
            let skipped = row.last.configuration_revision == metadata.configuration_revision
                && row.last.sequence.saturating_add(1) != metadata.sequence;
            if !had_gap && (skipped || arrived_ns.saturating_sub(row.last_ns) > row.max_gap_ns) {
                interrupted = true;
                let first = if skipped {
                    row.last_ns.saturating_add(1)
                } else {
                    row.last_ns.saturating_add(row.max_gap_ns)
                };
                self.gap(
                    source.clone(),
                    first,
                    if skipped {
                        GapReason::SkippedFrames
                    } else {
                        GapReason::NoData
                    },
                );
            }
        }
        self.gaps
            .iter_mut()
            .filter(|gap| gap.source == *source && gap.end_ns.is_none())
            .for_each(|gap| {
                gap.end_ns = Some(arrived_ns);
            });
        let max_gap_ns = max_gap.as_nanos().min(u128::from(u64::MAX)) as u64;
        if !interrupted
            && let Some(index) = prior
            && self.intervals[index].payload == id
            && self.intervals[index].last.configuration_revision == metadata.configuration_revision
        {
            let mut row = self.intervals.remove(index).unwrap();
            row.last_ns = arrived_ns;
            row.last = metadata;
            row.max_gap_ns = max_gap_ns;
            self.intervals.push_back(row);
            self.trim(arrived_ns);
            return true;
        }
        if bytes.len() as u64 + ROW_BYTES + PAYLOAD_OVERHEAD > self.limits.max_bytes {
            self.gap(source.clone(), arrived_ns, GapReason::Capacity);
            self.evicted = self.evicted.saturating_add(1);
            self.trim(arrived_ns);
            return false;
        }
        let payload = self.payloads.entry(id).or_insert_with(|| {
            self.bytes += bytes.len() as u64 + PAYLOAD_OVERHEAD;
            Payload {
                bytes,
                references: 0,
            }
        });
        payload.references += 1;
        self.intervals.push_back(Interval {
            first_ns: arrived_ns,
            last_ns: arrived_ns,
            first: metadata.clone(),
            last: metadata,
            payload: id,
            max_gap_ns,
        });
        self.bytes += ROW_BYTES;
        self.trim(arrived_ns);
        true
    }

    pub fn gap(&mut self, source: SourceKey, at: u64, reason: GapReason) {
        if source.validate().is_err() {
            return;
        }
        if let Some(gap) = self
            .gaps
            .iter_mut()
            .rev()
            .find(|gap| gap.source == source && gap.end_ns.is_none())
        {
            if gap.reason == reason {
                return;
            }
            gap.end_ns = Some(at.max(gap.first_ns));
        }
        self.gaps.push_back(Gap {
            source,
            first_ns: at,
            end_ns: None,
            reason,
        });
        self.bytes += ROW_BYTES;
        self.trim(at);
    }

    pub fn trim(&mut self, now: u64) {
        let floor = now
            .saturating_sub(self.limits.duration_ms * 1_000_000)
            .max(self.retention_floor);
        self.retention_floor = floor;
        while self
            .intervals
            .front()
            .is_some_and(|row| row.last_ns < floor)
        {
            self.remove_interval(true);
        }
        self.gaps.retain(|gap| {
            let keep = gap.end_ns.is_none_or(|end| end >= floor);
            if !keep {
                self.bytes -= ROW_BYTES;
            }
            keep
        });
        while self.bytes > self.limits.max_bytes
            || self.intervals.len() + self.gaps.len() > MAX_ROWS
        {
            let gap = self
                .gaps
                .iter()
                .enumerate()
                .min_by_key(|(_, gap)| gap.end_ns.unwrap_or(now));
            if !self.intervals.is_empty()
                && gap.is_none_or(|(_, gap)| self.intervals[0].last_ns <= gap.end_ns.unwrap_or(now))
            {
                self.remove_interval(false);
            } else if let Some((index, _)) = gap {
                let gap = self.gaps.remove(index).unwrap();
                self.bytes -= ROW_BYTES;
                self.capacity_before = Some(
                    self.capacity_before
                        .unwrap_or(0)
                        .max(gap.end_ns.unwrap_or(now)),
                );
                self.evicted = self.evicted.saturating_add(1);
            } else {
                break;
            }
        }
    }

    fn remove_interval(&mut self, expired: bool) {
        let row = self.intervals.pop_front().unwrap();
        self.bytes -= ROW_BYTES;
        let payload = self.payloads.get_mut(&row.payload).unwrap();
        payload.references -= 1;
        if payload.references == 0 {
            let payload = self.payloads.remove(&row.payload).unwrap();
            self.bytes -= payload.bytes.len() as u64 + PAYLOAD_OVERHEAD;
        }
        if !expired {
            self.capacity_before = Some(
                self.capacity_before
                    .unwrap_or(0)
                    .max(row.last_ns.saturating_add(row.max_gap_ns)),
            );
        }
        self.evicted = self.evicted.saturating_add(1);
    }

    pub fn at(&mut self, at: u64, now: u64, selected: &[SourceKey]) -> HistorySnapshot {
        self.trim(now);
        let floor = self.retention_floor;
        let images = selected
            .iter()
            .take(8)
            .filter(|source| source.validate().is_ok())
            .map(|source| {
                let mut result = HistoricalImage {
                    source: source.clone(),
                    state: HistoryState::NotObserved,
                    first_observed_ns: None,
                    last_confirmed_ns: None,
                    first_capture: None,
                    last_capture: None,
                    payload: None,
                };
                if at > now {
                    return result;
                }
                if at < floor {
                    result.state = HistoryState::Expired;
                    return result;
                }
                if let Some(gap) = self.gaps.iter().rev().find(|gap| {
                    gap.source == *source
                        && gap.first_ns <= at
                        && gap.end_ns.is_none_or(|end| at < end)
                }) {
                    result.state = HistoryState::Gap(gap.reason);
                    return result;
                }
                let row = self
                    .intervals
                    .iter()
                    .filter(|row| row.last.source == *source && row.first_ns <= at)
                    .max_by_key(|row| row.first_ns);
                if let Some(row) = row {
                    if at.saturating_sub(row.last_ns) > row.max_gap_ns {
                        result.state = HistoryState::Gap(GapReason::NoData);
                        return result;
                    }
                    result.state = if at <= row.last_ns {
                        HistoryState::Available
                    } else {
                        HistoryState::Sampled
                    };
                    result.first_observed_ns = Some(row.first_ns.max(floor));
                    result.last_confirmed_ns = Some(row.last_ns);
                    result.first_capture = Some(row.first.clone());
                    result.last_capture = Some(row.last.clone());
                    result.payload = self
                        .payloads
                        .get(&row.payload)
                        .map(|payload| payload.bytes.clone());
                } else if self.capacity_before.is_some_and(|before| at <= before) {
                    result.state = HistoryState::NotRetained;
                }
                result
            })
            .collect();
        HistorySnapshot {
            requested_ns: at,
            now_ns: now,
            retention_floor_ns: floor,
            images,
            selection_truncated: selected.len() > 8,
        }
    }

    pub fn stats(&self) -> HistoryStats {
        HistoryStats {
            accounted_bytes: self.bytes,
            payloads: self.payloads.len(),
            intervals: self.intervals.len(),
            gaps: self.gaps.len(),
            evicted_intervals: self.evicted,
        }
    }
}
