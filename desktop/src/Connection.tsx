import { useEffect, useState } from "react";
import { call, message } from "./api";
import type { Api, Session } from "./api";
import { Pairing } from "./Pairing";

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
  const [discovery, setDiscovery] = useState<Api["discovery"]>([]);
  const [searching, setSearching] = useState(false);
  const [searched, setSearched] = useState(false);
  const [pairOpen, setPairOpen] = useState(false);
  const route = (id: string, found = discovery) =>
    found.find((provider) => provider.advertised_fingerprint === id)
      ?.addresses[0] ?? "";
  useEffect(() => {
    let alive = true;
    void call<[string, string][]>("peers")
      .then((peers) => {
        if (alive) {
          setPeers(peers);
          setPeer(peers[0]?.[0] ?? "");
          setPairOpen(peers.length === 0);
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
      <div className="nearby">
        <button
          disabled={searching || busy}
          onClick={() => {
            setSearching(true);
            setError("");
            void call<Api["discovery"]>("discover")
              .then((found) => {
                setDiscovery(found);
                setSearched(true);
                if (peer && !address) setAddress(route(peer, found));
              })
              .catch((error) => setError(message(error)))
              .finally(() => setSearching(false));
          }}
        >
          {searching ? "searching…" : "find nearby"}
        </button>
        {searched && !discovery.length && (
          <p className="muted">nothing found · use an address below</p>
        )}
        {discovery.map((provider) =>
          provider.addresses.map((address) => {
            const known = peers.find(
              ([id]) => id === provider.advertised_fingerprint,
            );
            return (
              <button
                className="nearby-computer"
                key={`${provider.advertised_fingerprint}/${address}`}
                title={provider.advertised_fingerprint}
                onClick={() => {
                  setPeer(known?.[0] ?? "");
                  setAddress(known ? address : "");
                  if (!known) setPairOpen(true);
                }}
              >
                {known?.[1] ?? "new computer"} · {address}
              </button>
            );
          }),
        )}
      </div>
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
            onChange={(event) => {
              setPeer(event.target.value);
              setAddress(route(event.target.value));
            }}
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
      {peer && (
        <button
          className="forget"
          disabled={busy}
          onClick={() => {
            setBusy(true);
            setError("");
            void call("forget", { peer })
              .then(() => {
                setPeers((old) => old.filter(([id]) => id !== peer));
                setPeer("");
                setAddress("");
                setPairOpen(true);
              })
              .catch((error) => setError(message(error)))
              .finally(() => setBusy(false));
          }}
        >
          forget this computer
        </button>
      )}
      <details
        open={pairOpen}
        onToggle={(event) => setPairOpen(event.currentTarget.open)}
      >
        <summary>pair a computer</summary>
        <Pairing
          paired={(entry) => {
            setPeers((old) => [
              ...old.filter(([id]) => id !== entry[0]),
              entry,
            ]);
            setPeer(entry[0]);
            setAddress(route(entry[0]));
            setPairOpen(false);
          }}
        />
      </details>
      {error && (
        <p className="error" role="alert">
          {error}
        </p>
      )}
    </section>
  );
}
