import { useEffect, useState } from "react";
import { call, key, message } from "./api";
import type { Api, Session, Settings, Source } from "./api";

export function Speech({
  session,
  settings,
  sources,
  disabled,
}: {
  session: Session;
  settings: Settings;
  sources: Source[];
  disabled: boolean;
}) {
  const [choice, setChoice] = useState("large-v3-turbo");
  const [custom, setCustom] = useState("");
  const [models, setModels] = useState<Api["models"]>([]);
  const [refresh, setRefresh] = useState(0);
  const [speech, setSpeech] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    let alive = true;
    void (async () => {
      do {
        try {
          const rows = await call<Api["models"]>("models");
          if (!alive) break;
          setModels(rows);
          setChoice((current) =>
            current === "custom" ||
            !rows.length ||
            rows.some((model) => model.id === current)
              ? current
              : (rows.find((model) => model.phase === "ready")?.id ?? rows[0].id),
          );
          if (
            !rows.some(
              (row) => row.phase === "downloading" || row.phase === "verifying",
            )
          )
            break;
        } catch (error) {
          if (alive) setError(message(error));
          break;
        }
        await new Promise((resolve) => setTimeout(resolve, 1000));
      } while (alive);
    })();
    return () => {
      alive = false;
    };
  }, [refresh]);
  const selected = models.find((model) => model.id === choice);
  const model = choice === "custom" ? custom : (selected?.path ?? "");
  const downloading = models.find((model) => model.phase === "downloading");
  const action = async (name: string, args: unknown) => {
    setBusy(true);
    setError("");
    try {
      await call(name, args);
    } catch (error) {
      setError(message(error));
    } finally {
      setBusy(false);
      setRefresh((old) => old + 1);
    }
  };
  return (
    <>
      <details>
        <summary>speech</summary>
        <form
          onSubmit={(event) => {
            event.preventDefault();
            const selected = settings.audio.filter((source) =>
              speech.includes(key(source)),
            );
            void action("transcribe", {
              binding: session.binding,
              model,
              threads: 4,
              sources: selected,
            } satisfies Api["transcribe_request"]);
          }}
        >
          {settings.audio.map((audio) => (
            <label className="inline" key={key(audio)}>
              <input
                type="checkbox"
                checked={speech.includes(key(audio))}
                onChange={(event) =>
                  setSpeech(
                    event.target.checked
                      ? [...speech, key(audio)]
                      : speech.filter((id) => id !== key(audio)),
                  )
                }
              />
              {sources.find((source) => key(source.key) === key(audio))
                ?.label ?? audio.id}
            </label>
          ))}
          <label>
            model
            <select
              value={choice}
              onChange={(event) => setChoice(event.target.value)}
              disabled={busy}
            >
              {models.map((model) => (
                <option value={model.id} key={model.id}>
                  {model.id} · {Math.ceil(model.bytes / 1_000_000)} mb
                  {model.phase === "ready"
                    ? " · ready"
                    : model.phase === "verifying"
                      ? " · verifying…"
                      : ""}
                </option>
              ))}
              {!models.length && (
                <option value="large-v3-turbo">loading…</option>
              )}
              <option value="custom">use a file</option>
            </select>
          </label>
          {choice === "custom" ? (
            <label>
              model path
              <input
                value={custom}
                onChange={(event) => setCustom(event.target.value)}
                required
                spellCheck={false}
              />
            </label>
          ) : (
            <>
              {selected && selected.phase !== "ready" && (
                <button
                  type="button"
                  disabled={
                    busy || !!downloading || selected.phase === "verifying"
                  }
                  onClick={() =>
                    void action("model-install", {
                      id: choice,
                    } satisfies Api["model_install_request"])
                  }
                >
                  download {choice}
                </button>
              )}
              {downloading && (
                <div className="button-row">
                  <span className="muted" role="status">
                    {downloading.id} ·{" "}
                    {Math.floor(
                      (downloading.received_bytes / downloading.bytes) * 100,
                    )}
                    %
                  </span>
                  <button
                    type="button"
                    disabled={busy}
                    onClick={() => void action("model-cancel", {})}
                  >
                    cancel download
                  </button>
                </div>
              )}
              {selected?.error && (
                <p className="error" role="alert">
                  {selected.error}
                </p>
              )}
            </>
          )}
          <div className="button-row">
            <button disabled={busy || disabled || !speech.length || !model}>
              start
            </button>
            <button
              type="button"
              disabled={busy}
              onClick={() =>
                void action("transcription-stop", { binding: session.binding })
              }
            >
              stop speech
            </button>
          </div>
        </form>
      </details>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </>
  );
}
