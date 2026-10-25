import { useEffect, useRef, useState } from "react";
import { call, key, message, release } from "./api";
import type { Api, Session, Settings, Source, SourceKey } from "./api";
import { loadAudioClips, audioHistoryMessage } from "./audioHistory";

export function AudioArchive({
  session,
  settings,
  sources,
  archive,
  disabled,
}: {
  session: Session;
  settings: Settings;
  sources: Source[];
  archive: Api["observation"]["observation"]["archive"];
  disabled: boolean;
}) {
  const [busy, setBusy] = useState(false);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [ago, setAgo] = useState(0);
  const [seconds, setSeconds] = useState(10);
  const [clips, setClips] = useState<
    { url: string; name: string; id: string }[]
  >([]);
  const [result, setResult] = useState("");
  const [chosen, setChosen] = useState<SourceKey[]>([]);
  const actual = useRef(archive?.raw_audio_sources ?? []);
  actual.current = archive?.raw_audio_sources ?? [];
  const chosenKeys = actual.current.map(key).join(",");
  useEffect(() => setChosen(actual.current), [chosenKeys]);
  const epoch = useRef(session.binding.epoch);
  epoch.current = session.binding.epoch;
  const generation = useRef(0);
  useEffect(() => {
    setClips([]);
    setResult("");
    setChosen(actual.current);
    setBusy(false);
    setLoading(false);
    generation.current++;
    return () => {
      generation.current++;
    };
  }, [session.binding.epoch]);
  useEffect(
    () => () => {
      for (const clip of clips) URL.revokeObjectURL(clip.url);
    },
    [clips],
  );
  const selected = chosen.filter((source) =>
    settings.audio.some((audio) => key(audio) === key(source)),
  );
  return (
    <details>
      <summary>audio archive</summary>
      {settings.audio.map((source) => (
        <label className="inline" key={key(source)}>
          <input
            type="checkbox"
            checked={selected.some((audio) => key(audio) === key(source))}
            disabled={busy || disabled || !archive}
            onChange={(event) => {
              setBusy(true);
              setError("");
              const next = event.target.checked
                ? [...selected, source]
                : selected.filter((audio) => key(audio) !== key(source));
              setChosen(next);
              const request = epoch.current;
              void call("raw-audio", {
                binding: session.binding,
                sources: next,
              } satisfies Api["raw_audio_request"])
                .catch((error) => {
                  if (request === epoch.current) {
                    setError(message(error));
                    setChosen(actual.current);
                  }
                })
                .finally(() => {
                  if (request === epoch.current) setBusy(false);
                });
            }}
          />
          keep{" "}
          {sources.find((item) => key(item.key) === key(source))?.label ??
            source.id}
        </label>
      ))}
      {!settings.audio.length && (
        <p className="muted">choose an audio source to record</p>
      )}
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (loading) return;
          setLoading(true);
          setError("");
          setResult("");
          setClips([]);
          const request = ++generation.current;
          void (async () => {
            const newer_ms = Math.round(ago * 60_000);
            const response = await call<Api["audio_history"]>("audio-history", {
              binding: session.binding,
              older_ms: newer_ms + Math.round(seconds * 1000),
              newer_ms,
              sources: [],
            } satisfies Api["transcripts_request"]);
            if (request !== generation.current) {
              await release(response.media.map((item) => item.reference.id));
              return;
            }
            const created = await loadAudioClips(
              response,
              sources,
              () => request === generation.current,
            );
            if (request === generation.current) {
              setClips(created);
              setResult(audioHistoryMessage(response, created.length));
            } else for (const clip of created) URL.revokeObjectURL(clip.url);
          })()
            .catch((error) => {
              if (request === generation.current) setError(message(error));
            })
            .finally(() => {
              if (request === generation.current) setLoading(false);
            });
        }}
      >
        <label>
          minutes ago
          <input
            type="number"
            min={0}
            max={43200}
            step={0.01}
            value={ago}
            onChange={(event) => setAgo(Number(event.target.value))}
            required
          />
        </label>
        <label>
          seconds
          <input
            type="number"
            min={1}
            max={60}
            value={seconds}
            onChange={(event) => setSeconds(Number(event.target.value))}
            required
          />
        </label>
        <button disabled={loading || !archive}>
          {loading ? "loading…" : "load audio"}
        </button>
      </form>
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
      {result && <p className="muted">{result}</p>}
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </details>
  );
}
