import { Transcripts } from "./Transcripts";
import { useEffect, useState } from "react";
import { call, key, message, release } from "./api";
import type { Api } from "./api";
import { Connection } from "./Connection";
import { Controls } from "./Controls";
import { History } from "./History";
import type { Past } from "./History";
import { useSession } from "./useSession";
import { useHost } from "./useHost";
import { Recordings } from "./Recordings";

export function App() {
  const host = useHost();
  const { session, ready } = host;
  const [connecting, setConnecting] = useState(false);
  const [recordings, setRecordings] = useState(false);
  const [preview, setPreview] = useState("");
  const [past, setPast] = useState<Past | null>(null);
  const [error, setError] = useState("");
  const [stopping, setStopping] = useState(false);
  const live = useSession(session, preview, !!past || recordings);
  useEffect(
    () => () => {
      if (past?.url) URL.revokeObjectURL(past.url);
    },
    [past?.url],
  );

  const settings = live.observed?.settings;
  const images = settings?.images ?? [];
  const imageKeys = images.map((image) => key(image.source)).join(",");
  useEffect(() => {
    setPreview((old) =>
      images.some((image) => key(image.source) === old)
        ? old
        : images[0]
          ? key(images[0].source)
          : "",
    );
    setPast(null);
  }, [imageKeys, session?.binding.epoch]);
  const source = images.find((image) => key(image.source) === preview)?.source;
  const image = live.observed?.images.find(
    (image) => key(image.metadata.source) === preview,
  );
  const sourceName =
    live.sources?.sources.find((item) => key(item.key) === preview)?.label ??
    "preview";
  const status =
    live.observed?.connection ?? session?.connection ?? "disconnected";
  const text = past
    ? past.transcripts.map((entry) => entry.transcript)
    : live.transcripts;
  const previewUrl = past
    ? past.url
    : live.image?.source === preview
      ? live.image.url
      : null;

  return (
    <div className="app">
      <header className="app-header">
        <h1>liazo</h1>
        <span className="connection-state">
          {!ready ? "opening…" : session ? status : "disconnected"}
        </span>
        <div className="header-actions">
          <button
            disabled={!ready}
            aria-pressed={recordings}
            onClick={() => {
              setRecordings((value) => !value);
              setConnecting(false);
            }}
          >
            {recordings ? (session ? "live" : "connect") : "recordings"}
          </button>
          {session && (
            <>
              <button
                onClick={() => {
                  setRecordings(false);
                  setConnecting((old) => !old);
                }}
              >
                {connecting ? "back" : "connect"}
              </button>
              <button
                disabled={stopping || status !== "connected"}
                title="stop the shared capture session"
                onClick={() => {
                  setStopping(true);
                  setError("");
                  void call("stop", { binding: session.binding })
                    .catch((error) => setError(message(error)))
                    .finally(() => setStopping(false));
                }}
              >
                {stopping ? "stopping…" : "stop"}
              </button>
            </>
          )}
        </div>
      </header>
      {(error || host.error) && (
        <div className="error-banner" role="alert">
          <span>{error || host.error}</span>
          <button onClick={() => (host.error ? host.retry() : setError(""))}>
            retry
          </button>
        </div>
      )}
      {!ready ? (
        <main className="empty" aria-busy="true">
          opening local session…
        </main>
      ) : recordings ? (
        <Recordings />
      ) : !session || connecting ? (
        <main className="connection-main">
          <Connection
            connected={(next) => {
              host.connected(next);
              setConnecting(false);
            }}
          />
        </main>
      ) : (
        <>
          <div className="workspace">
            {settings && live.sources ? (
              <Controls
                session={session}
                settings={settings}
                sources={live.sources.sources}
                issues={live.sources.issues}
                revision={live.observed!.configuration_revision}
                observed={live.observed!}
              />
            ) : (
              <aside className="controls" aria-busy="true">
                loading sources…
              </aside>
            )}
            <main className="content">
              <div className="preview-heading">
                <label>
                  preview
                  <select
                    value={preview}
                    onChange={(event) => {
                      setPast(null);
                      setPreview(event.target.value);
                    }}
                    disabled={!images.length}
                  >
                    {!images.length && (
                      <option value="">no image source selected</option>
                    )}
                    {images.map((image) => (
                      <option key={key(image.source)} value={key(image.source)}>
                        {live.sources?.sources.find(
                          (source) => key(source.key) === key(image.source),
                        )?.label ?? image.source.id}
                      </option>
                    ))}
                  </select>
                </label>
                <span className="muted">
                  {past
                    ? past.state
                    : (image?.freshness.replaceAll("_", " ") ?? "waiting")}
                </span>
                <button
                  disabled={!source || status !== "connected" || !!past}
                  onClick={() =>
                    void call<Api["observation"]>("capture", {
                      binding: session.binding,
                      sources: [source],
                    })
                      .then((response) =>
                        release(response.media.map((item) => item.id)),
                      )
                      .catch((error) => setError(message(error)))
                  }
                >
                  capture now
                </button>
              </div>
              <div className="preview">
                {previewUrl ? (
                  <img src={previewUrl} alt={sourceName} draggable={false} />
                ) : (
                  <p className="muted">
                    {past
                      ? past.state
                      : source
                        ? "waiting for an image"
                        : "choose a display or window"}
                  </p>
                )}
              </div>
              <History
                session={session}
                source={source}
                show={past}
                change={setPast}
              />
              <Transcripts
                key={past ? "history" : "live"}
                past={past}
                rows={text}
                sources={live.sources?.sources ?? []}
                status={live.observed?.transcription}
              />
              {live.error && (
                <p className="error" role="alert">
                  {live.error}
                </p>
              )}
            </main>
          </div>
          <footer>
            <span>
              {settings
                ? `${settings.images.length} image · ${settings.audio.length} audio selected`
                : "loading"}
            </span>
            <span>
              {live.observed?.archive
                ? `archive ${live.observed.archive.phase} · ${live.observed.archive.dropped_records} dropped`
                : "archive off"}
            </span>
          </footer>
        </>
      )}
    </div>
  );
}
