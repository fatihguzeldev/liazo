import { useEffect, useRef, useState } from "react";
import { call, key, media, message, release } from "./api";
import type { Api, Session } from "./api";

type Transcript = Api["observation"]["observation"]["transcripts"][number];
const pause = () => new Promise((resolve) => setTimeout(resolve, 1000));

export function useSession(
  session: Session | null,
  preview: string,
  historical: boolean,
) {
  const [observed, setObserved] = useState<
    Api["observation"]["observation"] | null
  >(null);
  const [sources, setSources] = useState<Api["sources"] | null>(null);
  const [transcripts, setTranscripts] = useState<Transcript[]>([]);
  const [image, setImage] = useState<{ url: string; source: string } | null>(
    null,
  );
  const [error, setError] = useState("");
  const selected = useRef(preview);
  const fresh = useRef(true);
  const paused = useRef(historical);
  useEffect(() => {
    paused.current = historical;
    fresh.current = true;
    setImage(null);
  }, [historical]);
  useEffect(() => {
    selected.current = preview;
    fresh.current = true;
  }, [preview]);
  const bindingKey = session
    ? `${session.binding.session}/${session.binding.epoch}`
    : "";

  useEffect(() => {
    if (!session) {
      setObserved(null);
      setSources(null);
      setTranscripts([]);
      setImage(null);
      return;
    }
    let alive = true;
    let cursor = 0;
    let catalogRevision = -1;
    let url = "";
    const rows = new Map<string, Transcript>();
    fresh.current = true;
    setObserved(null);
    setSources(null);
    setTranscripts([]);
    setImage(null);
    setError("");
    const binding = session.binding;
    void (async () => {
      while (alive) {
        try {
          const current = fresh.current;
          fresh.current = false;
          const response = await call<Api["observation"]>("observe", {
            binding,
            after: cursor,
            current,
            wait_ms: 1000,
          });
          if (!alive) break;
          cursor = response.observation.cursor;
          setObserved(response.observation);
          if (catalogRevision !== response.observation.source_revision) {
            const catalog = await call<Api["sources"]>("sources", { binding });
            if (!alive) break;
            catalogRevision = response.observation.source_revision;
            setSources(catalog);
          }
          for (const row of response.observation.transcripts) {
            rows.delete(row.id);
            rows.set(row.id, row);
          }
          while (rows.size > 50) rows.delete(rows.keys().next().value!);
          setTranscripts([...rows.values()]);
          const reference = paused.current
            ? undefined
            : (response.media.find(
                (item) => key(item.source) === selected.current,
              ) ?? (!selected.current ? response.media[0] : undefined));
          await release(
            response.media
              .filter((item) => item.id !== reference?.id)
              .map((item) => item.id),
          );
          if (reference) {
            const bytes = await media(reference.id);
            if (!alive) break;
            const next = URL.createObjectURL(
              new Blob([bytes], { type: reference.content_type }),
            );
            const previous = url;
            url = next;
            setImage({ url: next, source: key(reference.source) });
            if (previous) URL.revokeObjectURL(previous);
          }
          setError("");
          if (response.observation.connection !== "connected") {
            const archive = response.observation.archive?.phase;
            if (
              !response.observation.transcription.active &&
              (!archive || archive === "stopped" || archive === "failed")
            )
              break;
            await pause();
          }
        } catch (error) {
          if (!alive) break;
          setError(message(error));
          fresh.current = true;
          await pause();
        }
      }
    })();
    return () => {
      alive = false;
      if (url) URL.revokeObjectURL(url);
    };
    // A reconnect creates a new epoch; consumer cursors belong to that epoch.
  }, [bindingKey]);
  const matching =
    !!session && observed?.binding.epoch === session.binding.epoch;
  return {
    observed: matching ? observed : null,
    sources: matching ? sources : null,
    transcripts: matching ? transcripts : [],
    image: matching ? image : null,
    error,
  };
}
