import { useCallback, useEffect, useRef, useState } from "react";
import { call, message } from "./api";
import type { Session } from "./api";

export function useHost() {
  const [session, setSession] = useState<Session | null>(null);
  const [ready, setReady] = useState(false);
  const [error, setError] = useState("");
  const [attempt, setAttempt] = useState(0);
  const generation = useRef(0);
  const connected = useCallback((session: Session) => {
    generation.current++;
    setSession(session);
  }, []);

  useEffect(() => {
    let alive = true;
    let initialized = false;
    setReady(false);
    setError("");
    void (async () => {
      while (alive) {
        try {
          const request = generation.current;
          const next = await call<Session | null>("status");
          if (!alive) break;
          if (request === generation.current)
            setSession((old) =>
              old?.binding.epoch === next?.binding.epoch &&
              old?.connection === next?.connection
                ? old
                : next,
            );
          initialized = true;
          setError("");
          setReady(true);
        } catch (error) {
          if (alive) {
            setError(message(error));
            setReady(true);
          }
          if (!initialized) break;
        }
        await new Promise((resolve) => setTimeout(resolve, 2000));
      }
    })();
    return () => {
      alive = false;
    };
  }, [attempt]);
  return {
    session,
    connected,
    ready,
    error,
    retry: () => setAttempt((old) => old + 1),
  };
}
