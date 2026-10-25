/* generated from Rust; run npm run bindings */

export type ImageFormat = "png" | "jpeg";
export type HistoryState =
  | {
      state: "available";
    }
  | {
      state: "sampled";
    }
  | {
      reason: GapReason;
      state: "gap";
    }
  | {
      state: "expired";
    }
  | {
      state: "not_retained";
    }
  | {
      state: "not_observed";
    };
export type GapReason =
  | "archive_backpressure"
  | "archive_failure"
  | "archive_stopped"
  | "archive_interrupted"
  | "disconnected"
  | "stopped"
  | "permission_required"
  | "unavailable"
  | "no_data"
  | "skipped_frames"
  | "capacity";
export type ModelPhase = "missing" | "downloading" | "ready" | "failed";
export type ArchivePhase = "recording" | "failed" | "stopping" | "stopped";
export type ConnectionState =
  "connecting" | "connected" | "stopped" | "disconnected";
export type FailureCode =
  | "invalid_request"
  | "unsupported_version"
  | "permission_required"
  | "source_missing"
  | "source_changed"
  | "unavailable"
  | "busy"
  | "timeout"
  | "internal";
export type Freshness =
  | "live"
  | "waiting"
  | "stale"
  | "unavailable"
  | "permission_required"
  | "disconnected"
  | "stopped";
export type TranscriptionGapReason =
  | "audio_discontinuity"
  | "invalid_audio"
  | "backpressure"
  | "engine_failure"
  | "interrupted";
export type EndReason = "silence" | "limit" | "stopped" | "gap";
export type SourceAvailability =
  "available" | "permission_required" | "unavailable";
export type SourceKind =
  "display" | "window" | "system_audio" | "process_audio" | "microphone";

export interface DesktopApi {
  capture_request: Capture;
  configure_request: Configure;
  connect_request: Connect;
  discovery: DiscoveredProvider[];
  forget_request: Forget;
  history: HistoryResponse;
  history_request: History;
  limits_request: Limits;
  model_install_request: ModelInstall;
  models: ModelStatus[];
  observation: ObservationResponse;
  observe_request: Observe;
  pair_request: Pair;
  scope: Scope;
  session: SessionSummary | null;
  sources: SourceSnapshot;
  transcribe_request: Transcribe;
  transcripts: TranscriptPage;
  transcripts_request: Transcripts;
}
export interface Capture {
  binding: SessionBinding;
  sources: SourceKey[];
}
export interface SessionBinding {
  epoch: string;
  session: string;
}
export interface SourceKey {
  generation: string;
  id: string;
}
export interface Configure {
  binding: SessionBinding;
  settings: CaptureSettings;
}
export interface CaptureSettings {
  audio: SourceKey[];
  images: ImageSubscription[];
}
export interface ImageSubscription {
  crop: CropPixels | null;
  format: ImageFormat;
  include_cursor: boolean;
  interval_ms: number;
  max_width: number | null;
  quality: number;
  source: SourceKey;
}
export interface CropPixels {
  height: number;
  width: number;
  x: number;
  y: number;
}
export interface Connect {
  address: string;
  peer: string;
}
export interface DiscoveredProvider {
  addresses: string[];
  advertised_fingerprint: string;
  name: string;
}
export interface Forget {
  peer: string;
}
export interface HistoryResponse {
  history: HistorySnapshot;
  media: MediaRef[];
  media_omitted: number;
}
export interface HistorySnapshot {
  images: HistoricalImage[];
  now_ns: number;
  requested_ns: number;
  retention_floor_ns: number;
  selection_truncated: boolean;
}
export interface HistoricalImage {
  first_capture: ImageMeta | null;
  first_observed_ns: number | null;
  last_capture: ImageMeta | null;
  last_confirmed_ns: number | null;
  source: SourceKey;
  state: HistoryState;
}
export interface ImageMeta {
  capture_started_ns: number;
  captured_ns: number;
  configuration_revision: number;
  /**
   * @minItems 32
   * @maxItems 32
   */
  fingerprint: [
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number,
    number
  ];
  format: ImageFormat;
  height: number;
  sequence: number;
  source: SourceKey;
  width: number;
}
export interface MediaRef {
  content_type: string;
  id: string;
  length: number;
  sequence: number;
  source: SourceKey;
}
export interface History {
  age_ms: number;
  binding: SessionBinding;
  sources: SourceKey[];
}
export interface Limits {
  binding: SessionBinding;
  limits: HistoryLimits;
}
export interface HistoryLimits {
  duration_ms: number;
  max_bytes: number;
}
export interface ModelInstall {
  id: string;
}
export interface ModelStatus {
  bytes: number;
  error: string | null;
  id: string;
  path: string | null;
  phase: ModelPhase;
  received_bytes: number;
  sha256: string;
}
export interface ObservationResponse {
  media: MediaRef[];
  media_omitted: number;
  observation: Observation;
}
export interface Observation {
  archive: ArchiveStatus | null;
  audio: AudioMeta[];
  audio_ages: AudioAge[];
  binding: SessionBinding;
  configuration_revision: number;
  connection: ConnectionState;
  cursor: number;
  failures: [SourceKey, Failure][];
  gaps: number;
  images: ObservedImage[];
  settings: CaptureSettings;
  source_revision: number;
  transcript_deltas_lost: boolean;
  transcription: TranscriptionStatus;
  transcripts: Transcript[];
}
export interface ArchiveStatus {
  dropped_records: number;
  phase: ArchivePhase;
  queued_bytes: number;
  stats: ArchiveStats | null;
}
export interface ArchiveStats {
  accounted_bytes: number;
  intervals: number;
  metadata_budget_bytes: number;
  payloads: number;
  retention_floor_ns: number;
}
export interface AudioMeta {
  captured_ns: number;
  channels: number;
  discontinuity: boolean;
  frames: number;
  sample_position: number;
  sample_rate: number;
  sequence: number;
  source: SourceKey;
  stream_id: string;
}
export interface AudioAge {
  age: AgeRange;
  source: SourceKey;
}
export interface AgeRange {
  maximum_ms: number;
  minimum_ms: number;
}
export interface Failure {
  code: FailureCode;
  message: string;
}
export interface ObservedImage {
  age: AgeRange | null;
  freshness: Freshness;
  metadata: ImageMeta;
}
export interface TranscriptionStatus {
  active: boolean;
  gaps: number;
  last_gap: TranscriptionGapReason | null;
  sources: SourceKey[];
}
export interface Transcript {
  end_ns: number;
  ending: EndReason | null;
  id: string;
  revision: number;
  source: SourceKey;
  start_ns: number;
  stream_id: string;
  text: string;
}
export interface Observe {
  after: number;
  binding: SessionBinding;
  current: boolean;
  sources: SourceKey[];
  wait_ms: number;
}
export interface Pair {
  code: string;
  label: string;
  name: string;
}
export interface Scope {
  binding: SessionBinding;
}
export interface SessionSummary {
  address: string;
  binding: SessionBinding;
  connection: ConnectionState;
  provider: string;
}
export interface SourceSnapshot {
  issues: SourceIssue[];
  revision: number;
  sources: Source[];
}
export interface SourceIssue {
  availability: SourceAvailability;
  kind: SourceKind;
  message: string;
}
export interface Source {
  availability: SourceAvailability;
  key: SourceKey;
  kind: SourceKind;
  label: string;
  pixel_height: number | null;
  pixel_width: number | null;
  process_id: number | null;
  scale: number | null;
}
export interface Transcribe {
  binding: SessionBinding;
  model: string;
  sources: SourceKey[];
  threads: number;
}
export interface TranscriptPage {
  entries: ArchivedTranscript[];
  retention_floor_ns: number;
  truncated: boolean;
}
export interface ArchivedTranscript {
  received_ns: number;
  receiver_range: ReceiverTimeRange | null;
  transcript: Transcript;
}
export interface ReceiverTimeRange {
  earliest_ns: number;
  latest_ns: number;
}
export interface Transcripts {
  binding: SessionBinding;
  newer_ms: number;
  older_ms: number;
  sources: SourceKey[];
}
