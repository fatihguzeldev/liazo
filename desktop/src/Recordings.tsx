import { useEffect, useRef, useState } from "react";
import { call, key, media, message, release } from "./api";
import type { Api, SourceKey } from "./api";
import { loadAudioClips, audioHistoryMessage } from "./audioHistory";
import type { AudioClip } from "./audioHistory";
import type { Past } from "./History";
import { Transcripts } from "./Transcripts";

type Epoch = Api["archives"]["entries"][number];
function recordingError(error: unknown): string {
  const text = message(error);
  return text === "archive not found" ? "recording expired or removed" : text;
}
export function Recordings() {
  const [page, setPage] = useState<Api["archives"] | null>(null);
  const [offset, setOffset] = useState(0);
  const [refresh, setRefresh] = useState(0);
  const [selected, setSelected] = useState<Epoch | null>(null);
  const [error, setError] = useState("");
  useEffect(() => {
    let current = true;
    setPage(null);
    setSelected(null);
    setError("");
    void call<Api["archives"]>("archives", { offset })
      .then((next) => {
        if (current) {
          setPage(next);
          setSelected(next.entries[0] ?? null);
        }
      })
      .catch((error) => {
        if (current) setError(recordingError(error));
      });
    return () => {
      current = false;
    };
  }, [offset, refresh]);
  return (
    <main className="content recordings">
      <div className="preview-heading">
        <label>
          recording
          <select
            value={selected?.binding.epoch ?? ""}
            disabled={!page?.entries.length}
            onChange={(event) =>
              setSelected(
                page?.entries.find(
                  (entry) => entry.binding.epoch === event.target.value,
                ) ?? null,
              )
            }
          >
            {!page?.entries.length && (
              <option value="">
                {page ? "no retained recordings" : "loading…"}
              </option>
            )}
            {page?.entries.map((entry) => (
              <option key={entry.binding.epoch} value={entry.binding.epoch}>
                {new Date(entry.wall_anchor_unix_ms).toLocaleString()} ·{" "}
                {entry.binding.epoch.slice(0, 8)}
                {entry.writer_active ? " · recording" : ""}
              </option>
            ))}
          </select>
        </label>
        <button onClick={() => setRefresh((value) => value + 1)}>
          refresh
        </button>
        <button
          disabled={!offset}
          onClick={() => setOffset((value) => Math.max(0, value - 128))}
        >
          previous
        </button>
        <button
          disabled={!page?.truncated || offset >= 65408}
          onClick={() => setOffset((value) => value + 128)}
        >
          next
        </button>
      </div>
      {!!page?.unavailable && (
        <p className="muted">{page.unavailable} recordings unavailable</p>
      )}
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      {selected && <Recording key={selected.binding.epoch} epoch={selected} />}
    </main>
  );
}

function Recording({ epoch }: { epoch: Epoch }) {
  const [sources, setSources] = useState<Api["archive_sources"] | null>(null);
  const [source, setSource] = useState<SourceKey>();
  const [minutes, setMinutes] = useState(0.01);
  const [past, setPast] = useState<Past | null>(null);
  const [clips, setClips] = useState<AudioClip[]>([]);
  const [audioState, setAudioState] = useState("");
  const [textState, setTextState] = useState("");
  const [busy, setBusy] = useState(false);
  const [loadingAudio, setLoadingAudio] = useState(false);
  const [error, setError] = useState("");
  const request = useRef(0);
  const audioRequest = useRef(0);
  const sourcePage = useRef(0);
  const endMs = Math.floor((epoch.closed_at_ns ?? epoch.latest_ns) / 1_000_000);
  const endTime = new Date(epoch.wall_anchor_unix_ms + endMs).toLocaleString();
  const loadSources = (after?: SourceKey) => {
    const version = ++sourcePage.current;
    void call<Api["archive_sources"]>("archive-sources", {
      binding: epoch.binding,
      after,
    })
      .then((next) => {
        if (version !== sourcePage.current) return;
        setSources(next);
        if (!after) setSource(next.entries.find((entry) => entry.image)?.key);
      })
      .catch((error) => {
        if (version === sourcePage.current) setError(recordingError(error));
      });
  };
  useEffect(() => {
    loadSources();
    return () => {
      request.current++;
      audioRequest.current++;
      sourcePage.current++;
    };
  }, []);
  useEffect(
    () => () => {
      if (past?.url) URL.revokeObjectURL(past.url);
    },
    [past?.url],
  );
  useEffect(
    () => () => {
      for (const clip of clips) URL.revokeObjectURL(clip.url);
    },
    [clips],
  );
  const clear = () => {
    request.current++;
    audioRequest.current++;
    setBusy(false);
    setLoadingAudio(false);
    setPast(null);
    setClips([]);
    setAudioState("");
    setTextState("");
    setError("");
  };
  const range = () => {
    const at_ms = endMs - Math.round(minutes * 60_000);
    if (
      !Number.isFinite(at_ms) ||
      !Number.isFinite(minutes) ||
      minutes < 0 ||
      at_ms < 0
    )
      throw new Error("time predates this epoch");
    return {
      at_ms,
      binding: epoch.binding,
      from_ms: Math.max(0, at_ms - 30_000),
      to_ms: Math.min(endMs, at_ms + 30_000),
    };
  };
  const load = async () => {
    clear();
    const version = ++request.current;
    setBusy(true);
    try {
      const target = range();
      const [image, text] = await Promise.allSettled([
        source
          ? call<Api["archive_images"]>("archive-images", {
              binding: target.binding,
              at_ms: target.at_ms,
              sources: [source],
            })
          : Promise.resolve(null),
        call<Api["transcripts"]>("archive-transcripts", {
          binding: target.binding,
          from_ms: target.from_ms,
          to_ms: target.to_ms,
        }),
      ]);
      if (image.status === "rejected") throw image.reason;
      const result = image.value;
      let url: string | null = null;
      try {
        if (result && version === request.current && result.media[0]) {
          const reference = result.media[0];
          const bytes = await media(reference.id);
          if (version === request.current)
            url = URL.createObjectURL(
              new Blob([bytes], { type: reference.content_type }),
            );
        }
      } finally {
        if (result)
          await release(result.media.map((item) => item.id)).catch(() => {});
      }
      if (version !== request.current) {
        if (url) URL.revokeObjectURL(url);
        return;
      }
      const state = result?.history.images[0]?.state;
      setPast({
        url,
        minutes,
        state: result?.history.media_omitted
          ? "image unavailable · retry"
          : state?.state === "gap"
            ? state.reason.replaceAll("_", " ")
            : (state?.state.replaceAll("_", " ") ?? "no retained images"),
        transcripts: text.status === "fulfilled" ? text.value.entries : [],
        truncated: text.status === "fulfilled" && text.value.truncated,
      });
      if (text.status === "rejected") setError(recordingError(text.reason));
      else if (target.to_ms * 1_000_000 < text.value.retention_floor_ns)
        setTextState("transcripts expired");
      else if (target.from_ms * 1_000_000 < text.value.retention_floor_ns)
        setTextState("part of this transcript range has expired");
    } catch (error) {
      if (version === request.current) setError(recordingError(error));
    } finally {
      if (version === request.current) setBusy(false);
    }
  };
  const loadAudio = async () => {
    const version = ++audioRequest.current;
    setLoadingAudio(true);
    setClips([]);
    setAudioState("");
    setError("");
    try {
      const target = range();
      const response = await call<Api["audio_history"]>("archive-audio", {
        binding: target.binding,
        from_ms: target.from_ms,
        to_ms: target.to_ms,
      });
      const created = await loadAudioClips(
        response,
        [],
        () => version === audioRequest.current,
      );
      if (version === audioRequest.current) {
        setClips(created);
        setAudioState(audioHistoryMessage(response, created.length));
      } else for (const clip of created) URL.revokeObjectURL(clip.url);
    } catch (error) {
      if (version === audioRequest.current) setError(recordingError(error));
    } finally {
      if (version === audioRequest.current) setLoadingAudio(false);
    }
  };
  const imageSources = sources?.entries.filter((entry) => entry.image) ?? [];
  return (
    <>
      <div className="history-bar">
        <label className="inline">
          image source
          <select
            value={source ? key(source) : ""}
            onChange={(event) => {
              clear();
              setSource(
                imageSources.find(
                  (entry) => key(entry.key) === event.target.value,
                )?.key,
              );
            }}
          >
            {!source && <option value="">no retained images</option>}
            {source &&
              !imageSources.some((entry) => key(entry.key) === key(source)) && (
                <option value={key(source)}>{source.id}</option>
              )}
            {imageSources.map((entry) => (
              <option key={key(entry.key)} value={key(entry.key)}>
                {entry.key.id} · {entry.key.generation.slice(0, 8)}
              </option>
            ))}
          </select>
        </label>
        {sources?.next_after && (
          <button onClick={() => loadSources(sources.next_after!)}>
            more sources
          </button>
        )}
      </div>
      <form
        className="history-bar"
        onSubmit={(event) => {
          event.preventDefault();
          void load();
        }}
      >
        <label className="inline">
          <input
            aria-label="minutes before selected time"
            type="number"
            min={0}
            max={43200}
            step={0.01}
            value={minutes}
            onChange={(event) => {
              clear();
              setMinutes(Number(event.target.value));
            }}
            required
          />
          min before
        </label>
        <span className="muted">
          {endTime}
          {epoch.closed_at_ns === null ? " · selected time" : " · end"}
        </span>
        <button disabled={busy || !sources}>
          {busy ? "loading…" : "load history"}
        </button>
        <button
          type="button"
          disabled={loadingAudio || !sources}
          onClick={() => void loadAudio()}
        >
          {loadingAudio ? "loading audio…" : "load audio"}
        </button>
      </form>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
      <div className="preview">
        {past?.url ? (
          <img
            src={past.url}
            alt={source?.id ?? "recorded image"}
            draggable={false}
          />
        ) : (
          <p className="muted">{past?.state ?? "choose a time"}</p>
        )}
      </div>
      {past && (
        <>
          <p className="muted">{past.state}</p>
          <Transcripts
            past={past}
            rows={past.transcripts.map((row) => row.transcript)}
            sources={[]}
            status={undefined}
          />
          {textState && <p className="muted">{textState}</p>}
        </>
      )}
      {clips.map((clip) => (
        <a
          className="audio-download"
          href={clip.url}
          download={`liazo-${clip.id}.wav`}
          key={clip.id}
        >
          {clip.name}
        </a>
      ))}
      {audioState && <p className="muted">{audioState}</p>}
    </>
  );
}
