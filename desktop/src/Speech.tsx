import { useState } from "react";
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
  const [model, setModel] = useState("");
  const [speech, setSpeech] = useState<string[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const action = async (name: string, args: unknown) => {
    setBusy(true);
    setError("");
    try {
      await call(name, args);
    } catch (error) {
      setError(message(error));
    } finally {
      setBusy(false);
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
            model path
            <input
              value={model}
              onChange={(event) => setModel(event.target.value)}
              placeholder="ggml-base.bin"
              required
              spellCheck={false}
            />
          </label>
          <div className="button-row">
            <button disabled={busy || disabled || !speech.length}>start</button>
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
