import { useState } from "react";
import { call, message } from "./api";
import type { Api } from "./api";

export function Pairing({
  paired,
}: {
  paired: (peer: [string, string]) => void;
}) {
  const [code, setCode] = useState("");
  const [name, setName] = useState("windows");
  const [label, setLabel] = useState("mac");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  return (
    <form
      onSubmit={(event) => {
        event.preventDefault();
        setBusy(true);
        setError("");
        void call<[string, string]>("pair", {
          code,
          name,
          label,
        } satisfies Api["pair_request"])
          .then((peer) => {
            setCode("");
            paired(peer);
          })
          .catch((error) => setError(message(error)))
          .finally(() => setBusy(false));
      }}
    >
      <label>
        pair code
        <input
          type="password"
          value={code}
          onChange={(event) => setCode(event.target.value)}
          required
          autoComplete="off"
          spellCheck={false}
          maxLength={32768}
          disabled={busy}
        />
      </label>
      <label>
        this computer
        <input
          value={name}
          onChange={(event) => setName(event.target.value)}
          required
          maxLength={128}
          disabled={busy}
        />
      </label>
      <label>
        remote computer
        <input
          value={label}
          onChange={(event) => setLabel(event.target.value)}
          required
          maxLength={128}
          disabled={busy}
        />
      </label>
      <button disabled={busy}>
        {busy ? "waiting for approval…" : "request pairing"}
      </button>
      {busy && (
        <p className="muted" role="status">
          approve on the other computer
        </p>
      )}
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </form>
  );
}
