import { useLayoutEffect, useRef } from "react";
import { key } from "./api";
import type { Api, Source } from "./api";
import type { Past } from "./History";

type Observation = Api["observation"]["observation"];
export function Transcripts({
  past,
  rows,
  sources,
  status,
}: {
  past: Past | null;
  rows: Observation["transcripts"];
  sources: Source[];
  status: Observation["transcription"] | undefined;
}) {
  const container = useRef<HTMLElement>(null);
  const following = useRef(!past);
  useLayoutEffect(() => {
    if (following.current && container.current)
      container.current.scrollTop = container.current.scrollHeight;
  }, [rows]);
  return (
    <section
      ref={container}
      className="transcripts"
      aria-labelledby="transcript-title"
      onScroll={(event) => {
        const element = event.currentTarget;
        following.current =
          element.scrollHeight - element.scrollTop - element.clientHeight < 48;
      }}
    >
      <div className="section-heading">
        <h2 id="transcript-title">
          {past ? "transcripts · history" : "transcripts"}
        </h2>
        <span className="muted">
          {past ? "" : status?.active ? "listening" : "off"}
        </span>
      </div>
      {!rows.length ? (
        <p className="muted">
          {past ? "no transcript results" : "no speech yet"}
        </p>
      ) : (
        <ol role="log" aria-live="polite">
          {rows.map((row) => (
            <li key={row.id} className={!row.ending ? "partial" : ""}>
              <small>
                {sources.find((source) => key(source.key) === key(row.source))
                  ?.label ?? row.source.id}{" "}
                ·{" "}
                {!row.ending
                  ? "partial"
                  : row.ending === "gap"
                    ? "interrupted"
                    : "final"}
              </small>
              <p>{row.text || "…"}</p>
            </li>
          ))}
        </ol>
      )}
      {past?.truncated && <p className="muted">more transcripts retained</p>}
      {!past && !!status?.gaps && (
        <p className="muted">
          {status.gaps} gaps · {status.last_gap?.replaceAll("_", " ")}
        </p>
      )}
    </section>
  );
}
