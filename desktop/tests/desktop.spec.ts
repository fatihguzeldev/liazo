import { expect, test } from "@playwright/test";

// Controlled IPC fixtures exercise the React UI, not native capture or Windows support.
test.beforeEach(async ({ page }) => {
  await page.addInitScript(() => {
    const id = (n: number) =>
      `00000000-0000-4000-8000-${n.toString().padStart(12, "0")}`;
    const source = { id: "display:fixture", generation: id(1) };
    const audio = { id: "audio:fixture", generation: id(2) };
    const binding = { session: id(3), epoch: id(4) };
    const canvas = document.createElement("canvas");
    canvas.width = 960;
    canvas.height = 540;
    const ctx = canvas.getContext("2d")!;
    ctx.fillStyle = "#252525";
    ctx.fillRect(0, 0, 960, 540);
    ctx.fillStyle = "#eeeeea";
    ctx.font = "24px monospace";
    ctx.fillText("fixture display", 40, 60);
    ctx.fillText("let count = 42;", 40, 120);
    const png = Uint8Array.from(
      atob(canvas.toDataURL("image/png").split(",")[1]),
      (character) => character.charCodeAt(0),
    ).buffer;
    const state = {
      denied: false,
      lastDenied: false,
      stopped: false,
      cursor: 2,
      revision: 1,
      calls: [] as { name: string; args: any }[],
      observations: 0,
      settings: {
        images: [
          {
            source,
            interval_ms: 500,
            max_width: 1920,
            crop: null,
            format: "png",
            quality: 90,
            include_cursor: false,
          },
        ],
        audio: [audio],
      },
      row: {
        id: id(5),
        stream_id: id(6),
        source: audio,
        revision: 1,
        start_ns: 1_000_000,
        end_ns: 2_000_000,
        text: "a partial question",
        ending: null as string | null,
      },
    };
    const metadata = {
      source,
      sequence: 1,
      configuration_revision: 1,
      capture_started_ns: 1_000_000,
      captured_ns: 2_000_000,
      width: 960,
      height: 540,
      format: "png",
      fingerprint: Array(32).fill(7),
    };
    const reference = {
      id: id(7),
      source,
      sequence: 1,
      content_type: "image/png",
      length: png.byteLength,
    };
    const observation = (current: boolean, after: number) => ({
      observation: {
        settings: state.settings,
        binding,
        cursor: state.cursor,
        connection: state.stopped ? "stopped" : "connected",
        configuration_revision: state.revision,
        source_revision: state.denied ? 2 : 1,
        images: state.settings.images.map(() => ({
          metadata,
          freshness: state.denied
            ? "permission_required"
            : state.stopped
              ? "stopped"
              : "live",
          age: { minimum_ms: 5, maximum_ms: 10 },
        })),
        audio: [],
        audio_ages: [{ source: audio, age: { minimum_ms: 5, maximum_ms: 10 } }],
        transcripts: after < state.cursor ? [state.row] : [],
        transcription: {
          active: true,
          sources: [audio],
          gaps: 0,
          last_gap: null,
        },
        transcript_deltas_lost: false,
        failures: state.denied
          ? [
              [
                source,
                {
                  code: "permission_required",
                  message: "screen permission required",
                },
              ],
            ]
          : [],
        gaps: 0,
        archive: {
          phase: "recording",
          queued_bytes: 0,
          dropped_records: 0,
          stats: null,
        },
      },
      media:
        state.settings.images.length && (current || after < 2)
          ? [reference]
          : [],
      media_omitted: 0,
    });
    const host = window as unknown as {
      fixture: typeof state;
      __TAURI_INTERNALS__: {
        invoke: (command: string, args: any) => Promise<any>;
      };
    };
    host.fixture = state;
    host.__TAURI_INTERNALS__ = {
      invoke: async (command, args) => {
        if (command === "media") return png;
        if (command === "release_media") return;
        const name = args.name;
        const input = args.args;
        state.calls.push({ name, args: input });
        if (name === "status")
          return {
            binding,
            provider: "a".repeat(64),
            address: "127.0.0.1:45810",
            connection: state.stopped ? "stopped" : "connected",
          };
        if (name === "peers") return [["a".repeat(64), "fixture mac"]];
        if (name === "sources")
          return {
            revision: state.denied ? 2 : 1,
            issues: [],
            sources: [
              {
                key: source,
                label: "fixture display",
                kind: "display",
                availability: state.denied
                  ? "permission_required"
                  : "available",
                pixel_width: 960,
                pixel_height: 540,
                scale: 1,
                process_id: null,
              },
              {
                key: audio,
                label: "fixture audio",
                kind: "system_audio",
                availability: "available",
                pixel_width: null,
                pixel_height: null,
                scale: null,
                process_id: null,
              },
            ],
          };
        if (name === "observe") {
          await new Promise((resolve) => setTimeout(resolve, 80));
          state.observations++;
          if (state.observations === 3) {
            state.row = {
              ...state.row,
              revision: 2,
              text: "a final question",
              ending: "silence",
            };
            state.cursor++;
          }
          if (state.denied !== state.lastDenied) {
            state.lastDenied = state.denied;
            state.cursor++;
          }
          return observation(input.current, input.after);
        }
        if (name === "configure") {
          state.settings = input.settings;
          state.cursor++;
          state.revision++;
          return { revision: state.revision };
        }
        if (name === "capture") return observation(true, 0);
        if (name === "history")
          return {
            history: {
              requested_ns: 1,
              now_ns: 3_600_000_000_000,
              retention_floor_ns: 0,
              selection_truncated: false,
              images: [
                {
                  source,
                  state: { state: "available" },
                  first_observed_ns: 1,
                  last_confirmed_ns: 2,
                  first_capture: metadata,
                  last_capture: metadata,
                },
              ],
            },
            media: [reference],
            media_omitted: 0,
          };
        if (name === "transcripts") {
          await new Promise((resolve) => setTimeout(resolve, 400));
          return {
            entries: [
              {
                transcript: {
                  ...state.row,
                  text: "history answer",
                  ending: "silence",
                },
                received_ns: 1,
                receiver_range: null,
              },
            ],
            truncated: false,
            retention_floor_ns: 0,
          };
        }
        if (name === "stop") {
          state.stopped = true;
          state.cursor++;
          return { stopped: true };
        }
        if (name === "transcription-stop") return { stopped: true };
        throw new Error(`unsupported fixture operation: ${name}`);
      },
    };
  });
});

test("source removal, permission errors and partial-to-final speech use the shared operations", async ({
  page,
}) => {
  await page.goto("/");
  await expect(
    page.getByRole("img", { name: "fixture display" }),
  ).toBeVisible();
  await expect
    .poll(() =>
      page
        .getByRole("img", { name: "fixture display" })
        .evaluate((image) => (image as HTMLImageElement).naturalWidth),
    )
    .toBe(960);
  await expect(
    page.getByText("a final question", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "capture now" }).click();
  await page.evaluate(() => {
    (window as any).fixture.denied = true;
  });
  await expect(
    page.getByText("screen permission required", { exact: true }),
  ).toBeVisible();
  const display = page.getByRole("checkbox", { name: /fixture display/ });
  await expect(display).toBeEnabled();
  await display.uncheck();
  await page.getByRole("button", { name: "apply", exact: true }).click();
  await expect(
    page.getByRole("button", { name: "capture now" }),
  ).toBeDisabled();
  await expect(page.getByText("choose a display or window")).toBeVisible();
});

test("returning live cancels late history presentation and the layout stays within the window", async ({
  page,
}) => {
  await page.goto("/");
  await expect(
    page.getByRole("img", { name: "fixture display" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "go", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "transcripts · history" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "live", exact: true }).click();
  await page.waitForTimeout(500);
  await expect(
    page.getByRole("heading", { name: "transcripts", exact: true }),
  ).toBeVisible();
  await expect(page.getByText("history answer", { exact: true })).toHaveCount(
    0,
  );
  for (const width of [720, 1024, 1440]) {
    await page.setViewportSize({ width, height: 800 });
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await expect(
      page.getByRole("button", { name: "capture now" }),
    ).toBeVisible();
  }
  await page.setViewportSize({ width: 1180, height: 800 });
  await page.screenshot({ path: "../context/ui-current.png" });
});
