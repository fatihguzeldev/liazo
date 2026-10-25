import { CaptureOptions } from "./CaptureOptions";
import { Speech } from "./Speech";
import { AudioArchive } from "./AudioArchive";
import { useEffect, useState } from "react";
import { call, key, message } from "./api";
import type { Api, Session, Settings, Source } from "./api";

export function Controls({
  session,
  settings,
  sources,
  issues,
  revision,
  observed,
}: {
  session: Session;
  settings: Settings;
  sources: Source[];
  issues: Api["sources"]["issues"];
  revision: number;
  observed: Api["observation"]["observation"];
}) {
  const [draft, setDraft] = useState(settings);
  const [dirty, setDirty] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    if (!dirty) setDraft(settings);
  }, [revision, dirty, settings]);
  const edit = (next: Settings) => {
    setDraft(next);
    setDirty(true);
  };
  const toggle = (source: Source) => {
    const id = key(source.key);
    if (source.kind === "display" || source.kind === "window") {
      const existing = draft.images.some((image) => key(image.source) === id);
      edit({
        ...draft,
        images: existing
          ? draft.images.filter((image) => key(image.source) !== id)
          : [
              ...draft.images,
              {
                source: source.key,
                interval_ms: 500,
                max_width: 1920,
                crop: null,
                format: "png",
                quality: 90,
                include_cursor: false,
              },
            ],
      });
    } else
      edit({
        ...draft,
        audio: draft.audio.some((audio) => key(audio) === id)
          ? draft.audio.filter((audio) => key(audio) !== id)
          : [...draft.audio, source.key],
      });
  };
  return (
    <aside className="controls" aria-label="capture settings">
      <form
        onSubmit={(event) => {
          event.preventDefault();
          setBusy(true);
          setError("");
          void call("configure", { binding: session.binding, settings: draft })
            .then(() => setDirty(false))
            .catch((error) => setError(message(error)))
            .finally(() => setBusy(false));
        }}
      >
        <div className="section-heading">
          <h2>sources</h2>
          <button className="primary small" disabled={!dirty || busy}>
            {busy ? "applying…" : "apply"}
          </button>
        </div>
        <div className="source-list">
          {sources.map((source) => {
            const checked =
              draft.images.some(
                (image) => key(image.source) === key(source.key),
              ) || draft.audio.some((audio) => key(audio) === key(source.key));
            const failure = observed.failures.find(
              ([id]) => key(id) === key(source.key),
            )?.[1];
            const audio = observed.audio_ages.find(
              (audio) => key(audio.source) === key(source.key),
            );
            return (
              <label className="source" key={key(source.key)}>
                <input
                  type="checkbox"
                  checked={checked}
                  onChange={() => toggle(source)}
                  disabled={
                    busy || (!checked && source.availability !== "available")
                  }
                />
                <span>
                  <span className="source-name" title={source.label}>
                    {source.label}
                  </span>
                  <small>
                    {source.kind.replaceAll("_", " ")}
                    {source.availability !== "available"
                      ? ` · ${source.availability.replaceAll("_", " ")}`
                      : audio
                        ? ` · ${observed.connection !== "connected" ? observed.connection : audio.age.maximum_ms > 2000 ? "stale" : "receiving"}`
                        : ""}
                  </small>
                  {failure && (
                    <small className="error">{failure.message}</small>
                  )}
                </span>
              </label>
            );
          })}
          {!sources.length && <p className="muted">no sources available</p>}
          {issues.map((issue) => (
            <p
              className="error"
              role="alert"
              key={`${issue.kind}/${issue.availability}`}
            >
              {issue.kind.replaceAll("_", " ")} · {issue.message}
            </p>
          ))}
        </div>
        <CaptureOptions draft={draft} sources={sources} edit={edit} />
      </form>
      <Speech
        session={session}
        settings={settings}
        sources={sources}
        disabled={dirty || busy}
      />
      <AudioArchive
        session={session}
        settings={settings}
        sources={sources}
        archive={observed.archive}
        disabled={dirty || busy || observed.connection !== "connected"}
      />
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </aside>
  );
}
