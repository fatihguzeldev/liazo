import { invoke } from "@tauri-apps/api/core";
import type { DesktopApi } from "./bindings";

export type Api = DesktopApi;
export type Session = NonNullable<Api["session"]>;
export type Settings = Api["configure_request"]["settings"];
export type Source = Api["sources"]["sources"][number];
export type SourceKey = Source["key"];
export const key = (source: SourceKey) => `${source.id}/${source.generation}`;
export function call<T>(name: string, args: unknown = {}): Promise<T> {
  return invoke("operation", { name, args });
}
export const media = (id: string): Promise<ArrayBuffer> =>
  invoke("media", { id });
export const release = (ids: string[]): Promise<void> =>
  ids.length ? invoke("release_media", { ids }) : Promise.resolve();
export const message = (error: unknown) =>
  error instanceof Error ? error.message : String(error);
