import { key, media, release } from "./api";
import type { Api, Source } from "./api";

export type AudioClip = { url: string; name: string; id: string };
export async function loadAudioClips(
  response: Api["audio_history"],
  sources: Source[],
  current: () => boolean,
): Promise<AudioClip[]> {
  const created: AudioClip[] = [];
  try {
    for (const item of response.media) {
      if (!current()) break;
      const data = await media(item.reference.id);
      if (!current()) break;
      const segment = response.audio.clips.find(
        (clip) => clip.segment.id === item.segment,
      )?.segment;
      const duration = segment
        ? segment.packets.reduce((sum, packet) => sum + packet.frames, 0) /
          segment.sample_rate
        : 0;
      const label =
        sources.find((source) => key(source.key) === key(item.reference.source))
          ?.label ?? item.reference.source.id;
      created.push({
        id: item.segment,
        url: URL.createObjectURL(
          new Blob([data], { type: item.reference.content_type }),
        ),
        name: `${label} · ${duration.toFixed(duration < 1 ? 2 : 1)} s`,
      });
    }
    if (current()) return created;
    for (const clip of created) URL.revokeObjectURL(clip.url);
    return [];
  } catch (error) {
    for (const clip of created) URL.revokeObjectURL(clip.url);
    throw error;
  } finally {
    await release(response.media.map((item) => item.reference.id)).catch(
      () => {},
    );
  }
}

export function audioHistoryMessage(
  response: Api["audio_history"],
  count: number,
): string {
  const audio = response.audio;
  if (audio.last_ns < audio.retention_floor_ns) return "audio expired";
  if (audio.first_ns < audio.retention_floor_ns)
    return "part of this range has expired";
  if (audio.truncated) return "more audio exists · use a shorter range";
  if (response.media_omitted) return "some clips are unavailable";
  if (audio.gaps.length) return "archive gaps in this range";
  if (count) return "";
  if (!audio.policy.length) return "raw recording was off in this range";
  if (
    audio.capacity_before_ns !== null &&
    audio.first_ns <= audio.capacity_before_ns
  )
    return "audio no longer retained";
  return "no retained audio in this range";
}
