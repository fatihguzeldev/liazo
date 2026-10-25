import { useEffect, useState } from "react";
import { call, message } from "./api";
import type { Session } from "./api";

export function Connection({
  connected,
}: {
  connected: (session: Session) => void;
}) {
  const [peers, setPeers] = useState<[string, string][]>([]);
  const [peer, setPeer] = useState("");
  const [address, setAddress] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  useEffect(() => {
    let alive = true;
    void call<[string, string][]>("peers")
      .then((peers) => {
        if (alive) {
          setPeers(peers);
          setPeer(peers[0]?.[0] ?? "");
        }
      })
      .catch((error) => {
        if (alive) setError(message(error));
      });
    return () => {
      alive = false;
    };
  }, []);
  return (
    <section className="connect-pane" aria-labelledby="connect-title">
      <h2 id="connect-title">connect a computer</h2>
      <form
        onSubmit={(event) => {
          event.preventDefault();
          setBusy(true);
          setError("");
          void call<Session>("connect", { peer, address })
            .then(connected)
            .catch((error) => setError(message(error)))
            .finally(() => setBusy(false));
        }}
      >
        <label>
          computer
          <select
            value={peer}
            onChange={(event) => setPeer(event.target.value)}
            disabled={busy}
          >
            <option value="" disabled>
              {peers.length ? "choose a computer" : "no paired computers"}
            </option>
            {peers.map(([id, label]) => (
              <option key={id} value={id}>
                {label}
              </option>
            ))}
          </select>
        </label>
        <label>
          address
          <input
            value={address}
            onChange={(event) => setAddress(event.target.value)}
            placeholder="192.168.1.10:45810"
            required
            autoCapitalize="none"
            spellCheck={false}
            disabled={busy}
          />
        </label>
        <button className="primary" disabled={busy || !peer}>
          {busy ? "connecting…" : "connect"}
        </button>
      </form>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </section>
  );
}
