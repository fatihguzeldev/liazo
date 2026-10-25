import { useEffect, useRef, useState } from "react";
import { call, message } from "./api";
import type { Api, Session } from "./api";
import { Pairing } from "./Pairing";
import {
  forgetAddress,
  rememberAddress,
  rememberedAddress,
  rememberedProvider,
} from "./routing";

export function Connection({
  connected,
  session,
}: {
  connected: (session: Session) => void;
  session?: Session | null;
}) {
  const [peers, setPeers] = useState<[string, string][]>([]);
  const [{ peer, address }, setTarget] = useState({
    peer: "",
    address: "",
    edited: false,
  });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const [discovery, setDiscovery] = useState<Api["discovery"]>([]);
  const [searching, setSearching] = useState(false);
  const [searched, setSearched] = useState(false);
  const [pairOpen, setPairOpen] = useState(false);
  const searchRevision = useRef(0);
  const route = (id: string, found = discovery) =>
    found.find((provider) => provider.advertised_fingerprint === id)
      ?.addresses[0] ?? "";
  const knownAddress = (id: string) =>
    !id
      ? ""
      : session?.provider === id && !session.pending
        ? session.address
        : rememberedAddress(id);
  const discover = () => {
    const revision = ++searchRevision.current;
    setSearching(true);
    setError("");
    void call<Api["discovery"]>("discover")
      .then((found) => {
        if (revision !== searchRevision.current) return;
        setDiscovery(found);
        setSearched(true);
        setTarget((target) =>
          target.peer && !target.address && !target.edited
            ? { ...target, address: route(target.peer, found) }
            : target,
        );
      })
      .catch((error) => {
        if (revision === searchRevision.current) setError(message(error));
      })
      .finally(() => {
        if (revision === searchRevision.current) setSearching(false);
      });
  };
  const selectPeer = (id: string, selectedAddress?: string) => {
    const address = selectedAddress ?? (knownAddress(id) || route(id));
    setTarget({ peer: id, address, edited: false });
    if (id && !address) discover();
  };
  useEffect(() => {
    let alive = true;
    void call<[string, string][]>("peers")
      .then((peers) => {
        if (alive) {
          setPeers(peers);
          const preferred = session?.provider ?? rememberedProvider();
          const initial = peers.some(([id]) => id === preferred)
            ? preferred
            : (peers[0]?.[0] ?? "");
          selectPeer(initial);
          setPairOpen(peers.length === 0);
        }
      })
      .catch((error) => {
        if (alive) setError(message(error));
      });
    return () => {
      alive = false;
      searchRevision.current++;
    };
  }, []);
  return (
    <section className="connect-pane" aria-labelledby="connect-title">
      <h2 id="connect-title">connect a computer</h2>
      <div className="nearby">
        <button disabled={searching || busy} onClick={discover}>
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
                  selectPeer(known?.[0] ?? "", known ? address : "");
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
            .then((session) => {
              rememberAddress(session.provider, session.address);
              connected(session);
            })
            .catch((error) => setError(message(error)))
            .finally(() => setBusy(false));
        }}
      >
        <label>
          computer
          <select
            value={peer}
            onChange={(event) => selectPeer(event.target.value)}
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
            onChange={(event) => {
              const address = event.target.value;
              setTarget((target) => ({
                ...target,
                address,
                edited: true,
              }));
            }}
            placeholder="ip:port"
            required
            autoCapitalize="none"
            spellCheck={false}
            disabled={busy || !peer}
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
                forgetAddress(peer);
                setPeers((old) => old.filter(([id]) => id !== peer));
                setTarget({ peer: "", address: "", edited: false });
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
            selectPeer(entry[0]);
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
