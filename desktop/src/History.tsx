import { useEffect, useRef, useState } from "react";
import { call, media, message, release } from "./api";
import type { Api, Session, SourceKey } from "./api";

export interface Past {
  url: string | null;
  state: string;
  minutes: number;
  transcripts: Api["transcripts"]["entries"];
  truncated: boolean;
}

export function History({
  session,
  source,
  show,
  change,
}: {
  session: Session;
  source: SourceKey | undefined;
  show: Past | null;
  change: (past: Past | null) => void;
}) {
  const [minutes, setMinutes] = useState(12);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const generation = useRef(0);
  useEffect(
    () => () => {
      generation.current += 1;
    },
    [session.binding.epoch, source?.id, source?.generation],
  );
  return (
    <div className="history-bar">
      <button
        type="button"
        aria-pressed={!show}
        onClick={() => {
          generation.current += 1;
          change(null);
          setError("");
          setBusy(false);
        }}
      >
        live
      </button>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          if (!source) return;
          setBusy(true);
          setError("");
          const request = ++generation.current;
          void (async () => {
            const age_ms = Math.round(minutes * 60_000);
            const result = await call<Api["history"]>("history", {
              binding: session.binding,
              age_ms,
              sources: [source],
            });
            if (request !== generation.current) {
              await release(result.media.map((item) => item.id));
              return;
            }
            const reference = result.media[0];
            await release(result.media.slice(1).map((item) => item.id));
            const data = reference ? await media(reference.id) : null;
            if (request !== generation.current) return;
            const url =
              data && reference
                ? URL.createObjectURL(
                    new Blob([data], { type: reference.content_type }),
                  )
                : null;
            const state = result.history.images[0]?.state;
            const reason =
              state?.state === "gap"
                ? state.reason.replaceAll("_", " ")
                : (state?.state.replaceAll("_", " ") ?? "not observed");
            change({
              url,
              state: reason,
              minutes,
              transcripts: [],
              truncated: false,
            });
            const older_ms = Math.min(
              age_ms + 60_000,
              Math.floor(result.history.now_ns / 1_000_000),
            );
            const transcripts = await call<Api["transcripts"]>("transcripts", {
              binding: session.binding,
              older_ms,
              newer_ms: age_ms,
            });
            if (request === generation.current)
              change({
                url,
                state: reason,
                minutes,
                transcripts: transcripts.entries,
                truncated: transcripts.truncated,
              });
          })()
            .catch((error) => {
              if (request === generation.current) setError(message(error));
            })
            .finally(() => {
              if (request === generation.current) setBusy(false);
            });
        }}
      >
        <label className="inline">
          history
          <input
            aria-label="minutes ago"
            type="number"
            min={0.01}
            max={43200}
            step={0.01}
            value={minutes}
            onChange={(event) => setMinutes(Number(event.target.value))}
            required
          />
          min ago
        </label>
        <button disabled={!source || busy}>{busy ? "loading…" : "go"}</button>
      </form>
      {show && (
        <span className="muted">
          {show.minutes}m ago · {show.state}
        </span>
      )}
      {error && (
        <span className="error" role="alert">
          {error}
        </span>
      )}
    </div>
  );
}
