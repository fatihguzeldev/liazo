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
    const wav = new ArrayBuffer(698);
    const wave = new DataView(wav);
    const tag = (offset: number, text: string) =>
      [...text].forEach((char, index) =>
        wave.setUint8(offset + index, char.charCodeAt(0)),
      );
    tag(0, "RIFF");
    wave.setUint32(4, 690, true);
    tag(8, "WAVEfmt ");
    wave.setUint32(16, 18, true);
    wave.setUint16(20, 3, true);
    wave.setUint16(22, 1, true);
    wave.setUint32(24, 16000, true);
    wave.setUint32(28, 64000, true);
    wave.setUint16(32, 4, true);
    wave.setUint16(34, 32, true);
    tag(38, "fact");
    wave.setUint32(42, 4, true);
    wave.setUint32(46, 160, true);
    tag(50, "data");
    wave.setUint32(54, 640, true);
    for (let index = 58; index < wav.byteLength; index += 4)
      wave.setFloat32(index, 0.25, true);
    const state = {
      connected: !new URL(location.href).searchParams.has("unpaired"),
      peers: new URL(location.href).searchParams.has("unpaired")
        ? ([] as [string, string][])
        : ([["a".repeat(64), "fixture mac"]] as [string, string][]),
      approved: false,
      rawAudio: [] as (typeof audio)[],
      retainedAudio: false,
      audioExpired: false,
      archiveExpired: false,
      archiveDelay: false,
      recovery: [] as {
        operation: string;
        source: { id: string; generation: string } | null;
        message: string;
      }[],
      modelPhase: new URL(location.href).searchParams.has("verifying")
        ? "verifying"
        : "missing",
      modelReady: false,
      denied: false,
      lastDenied: false,
      stopped: false,
      disconnected: false,
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
        connection: state.stopped
          ? "stopped"
          : state.disconnected
            ? "disconnected"
            : "connected",
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
          raw_audio_sources: state.rawAudio,
          buffered_audio_bytes: 0,
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
        if (command === "media") return args.id === id(10) ? wav : png;
        if (command === "release_media") return;
        const name = args.name;
        const input = args.args;
        state.calls.push({ name, args: input });
        if (name === "status" || name === "connect") {
          if (name === "connect") {
            state.connected = true;
            state.disconnected = false;
            state.stopped = false;
          }
          return state.connected
            ? {
                binding,
                recovery: state.recovery,
                provider: "a".repeat(64),
                address: "127.0.0.1:45810",
                connection: state.stopped
                  ? "stopped"
                  : state.disconnected
                    ? "disconnected"
                    : "connected",
              }
            : null;
        }
        if (name === "peers") return state.peers;
        if (name === "raw-audio") {
          state.rawAudio = input.sources;
          state.retainedAudio ||= input.sources.length > 0;
          state.cursor++;
          return { applied: true };
        }
        if (name === "audio-history" || name === "archive-audio")
          return {
            audio: {
              first_ns: 0,
              last_ns: 3_600_000_000_000,
              retention_floor_ns: state.audioExpired ? 4_000_000_000_000 : 0,
              capacity_before_ns: null,
              truncated: false,
              gaps: [],
              policy:
                (state.retainedAudio || name === "archive-audio") &&
                !state.audioExpired
                  ? [{ source: audio, first_ns: 0, end_ns: null }]
                  : [],
              clips:
                (state.retainedAudio || name === "archive-audio") &&
                !state.audioExpired
                  ? [
                      {
                        segment: {
                          id: id(9),
                          source: audio,
                          stream_id: id(6),
                          sample_rate: 16000,
                          channels: 1,
                          packets: [
                            {
                              sequence: 1,
                              sample_position: 0,
                              frames: 160,
                              captured_ns: 1_000_000,
                              received_ns: 2_000_000,
                              receiver_range: null,
                              discontinuity: true,
                            },
                          ],
                        },
                      },
                    ]
                  : [],
            },
            media:
              (state.retainedAudio || name === "archive-audio") &&
              !state.audioExpired
                ? [
                    {
                      segment: id(9),
                      reference: {
                        id: id(10),
                        source: audio,
                        sequence: 1,
                        content_type: "audio/wav",
                        length: wav.byteLength,
                      },
                    },
                  ]
                : [],
            media_omitted: 0,
          };
        if (name === "archives")
          return {
            entries: [11, 12].map((epoch) => ({
              binding: { session: binding.session, epoch: id(epoch) },
              provider: "a".repeat(64),
              wall_anchor_unix_ms: 1_790_000_000_000 - epoch * 60_000,
              latest_ns: 1_200_000_000_000,
              closed_at_ns: 1_200_000_000_000,
              retention_floor_ns: 0,
              writer_active: false,
              limits: { duration_ms: 21_600_000, max_bytes: 17_179_869_184 },
              accounted_bytes: 10_000,
            })),
            truncated: false,
            unavailable: 0,
          };
        if (name === "archive-sources")
          return {
            entries: [
              { key: source, image: true, audio: false },
              { key: audio, image: false, audio: true },
            ],
            truncated: false,
            next_after: null,
          };
        if (name === "archive-images") {
          if (state.archiveDelay)
            await new Promise((resolve) => setTimeout(resolve, 500));
          return {
            history: {
              binding: input.binding,
              at_ns: input.at_ms * 1_000_000,
              images: [
                {
                  source,
                  state: {
                    state: state.archiveExpired ? "expired" : "sampled",
                  },
                  first_observed_ns: 1,
                  last_confirmed_ns: 2,
                  first_capture: metadata,
                  last_capture: metadata,
                },
              ],
              selection_truncated: false,
              media_omitted: 0,
            },
            media: state.archiveExpired ? [] : [reference],
          };
        }
        if (name === "archive-transcripts")
          return {
            entries: state.archiveExpired
              ? []
              : [
                  {
                    transcript: {
                      ...state.row,
                      text:
                        input.binding.epoch === id(11)
                          ? "previous connection"
                          : "older connection",
                      ending: "silence",
                    },
                    received_ns: 1,
                    receiver_range: null,
                  },
                ],
            truncated: false,
            retention_floor_ns: state.archiveExpired ? 2_000_000_000_000 : 0,
          };
        if (name === "models")
          return [
            {
              id: "base",
              bytes: 147951465,
              sha256: "fixture-hash",
              phase: state.modelReady ? "ready" : state.modelPhase,
              received_bytes: state.modelReady ? 147951465 : 0,
              path: state.modelReady
                ? "C:\\liazo\\models\\ggml-base.bin"
                : null,
              error: null,
            },
          ];
        if (name === "model-install") {
          state.modelPhase = "downloading";
          return { accepted: true };
        }
        if (name === "model-cancel") {
          state.modelPhase = "missing";
          return { cancelled: true };
        }
        if (name === "transcribe") return { started: true };
        if (name === "discover")
          return [
            {
              name: "untrusted name",
              addresses: ["192.168.1.10:45810"],
              advertised_fingerprint: "b".repeat(64),
            },
          ];
        if (name === "pair") {
          while (!state.approved)
            await new Promise((resolve) => setTimeout(resolve, 10));
          const entry: [string, string] = ["b".repeat(64), input.label];
          state.peers.push(entry);
          return entry;
        }
        if (name === "forget") {
          state.peers = state.peers.filter(([peer]) => peer !== input.peer);
          return { removed: true };
        }
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

test("an unpaired computer is discovered, approved, connected and forgotten", async ({
  page,
}) => {
  await page.goto("/?unpaired");
  await page.getByRole("button", { name: "find nearby" }).click();
  await page
    .getByRole("button", { name: "new computer · 192.168.1.10:45810" })
    .click();
  await page
    .getByLabel("pair code", { exact: true })
    .fill("fixture-invitation");
  await page.getByRole("button", { name: "request pairing" }).click();
  await expect(page.getByText("approve on the other computer")).toBeVisible();
  await expect(
    page.getByRole("button", { name: "connect", exact: true }),
  ).toBeDisabled();
  await page.evaluate(() => {
    (window as any).fixture.approved = true;
  });
  await expect(
    page.getByRole("combobox", { name: "computer", exact: true }),
  ).toHaveValue("b".repeat(64));
  await expect(page.getByLabel("address", { exact: true })).toHaveValue(
    "192.168.1.10:45810",
  );
  await page.getByRole("button", { name: "connect", exact: true }).click();
  await expect(
    page.getByRole("img", { name: "fixture display" }),
  ).toBeVisible();
  await page.getByRole("button", { name: "connect", exact: true }).click();
  await page.getByRole("button", { name: "forget this computer" }).click();
  await expect(
    page.getByRole("combobox", { name: "computer", exact: true }),
  ).toHaveValue("");
  await expect(
    page.getByRole("button", { name: "request pairing" }),
  ).toBeVisible();
});

test("an untrusted discovery cannot keep a previously selected paired identity", async ({
  page,
}) => {
  await page.goto("/");
  await page.getByRole("button", { name: "connect", exact: true }).click();
  await expect(
    page.getByRole("combobox", { name: "computer", exact: true }),
  ).toHaveValue("a".repeat(64));
  await page.getByRole("button", { name: "find nearby" }).click();
  await page
    .getByRole("button", { name: "new computer · 192.168.1.10:45810" })
    .click();
  await expect(
    page.getByRole("combobox", { name: "computer", exact: true }),
  ).toHaveValue("");
  await expect(page.getByLabel("address", { exact: true })).toHaveValue("");
  await expect(
    page
      .locator(".connect-pane")
      .getByRole("button", { name: "connect", exact: true }),
  ).toBeDisabled();
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

test("speech waits for model verification and a download can be cancelled", async ({
  page,
}) => {
  await page.goto("/?verifying");
  const speech = page
    .locator("details")
    .filter({ has: page.locator("summary", { hasText: /^speech$/ }) });
  await speech.locator("summary").click();
  await expect(
    speech.getByRole("button", { name: "download base", exact: true }),
  ).toBeDisabled();
  await page.evaluate(() => {
    (window as any).fixture.modelPhase = "missing";
  });
  await expect(
    speech.getByRole("button", { name: "download base", exact: true }),
  ).toBeEnabled();
  await speech
    .getByRole("checkbox", { name: "fixture audio", exact: true })
    .check();
  await expect(
    speech.getByRole("button", { name: "start", exact: true }),
  ).toBeDisabled();
  await speech
    .getByRole("button", { name: "download base", exact: true })
    .click();
  await expect(speech.getByText("base · 0%", { exact: true })).toBeVisible();
  await speech.getByRole("button", { name: "cancel download" }).click();
  await expect(
    speech.getByRole("button", { name: "download base", exact: true }),
  ).toBeEnabled();
  await speech
    .getByRole("button", { name: "download base", exact: true })
    .click();
  await page.evaluate(() => {
    (window as any).fixture.modelReady = true;
  });
  await expect(
    speech.getByRole("button", { name: "start", exact: true }),
  ).toBeEnabled();
  await speech.getByRole("button", { name: "start", exact: true }).click();
  await expect
    .poll(() =>
      page.evaluate(
        () =>
          (window as any).fixture.calls.find(
            (call: any) => call.name === "transcribe",
          )?.args.model,
      ),
    )
    .toBe("C:\\liazo\\models\\ggml-base.bin");
});

test("raw audio is opt in, downloads a WAV and distinguishes expiry from recording being off", async ({
  page,
}) => {
  await page.goto("/");
  const archive = page
    .locator("details")
    .filter({ has: page.locator("summary", { hasText: /^audio archive$/ }) });
  await archive.locator("summary").click();
  await expect(
    archive.getByRole("checkbox", { name: "keep fixture audio" }),
  ).not.toBeChecked();
  await archive.getByRole("button", { name: "load audio" }).click();
  await expect(
    archive.getByText("raw recording was off in this range"),
  ).toBeVisible();
  await archive.getByRole("checkbox", { name: "keep fixture audio" }).check();
  await expect(
    archive.getByRole("checkbox", { name: "keep fixture audio" }),
  ).toBeChecked();
  await archive.getByRole("button", { name: "load audio" }).click();
  const ready = page.waitForEvent("download");
  await archive.getByRole("link", { name: "fixture audio · 0.01 s" }).click();
  const stream = await (await ready).createReadStream();
  const chunks = [];
  for await (const chunk of stream!) chunks.push(chunk);
  const bytes = Buffer.concat(chunks);
  expect(bytes.subarray(0, 4).toString()).toBe("RIFF");
  expect(bytes.readUInt16LE(20)).toBe(3);
  expect(bytes.length).toBe(698);
  await page.evaluate(() => {
    (window as any).fixture.audioExpired = true;
  });
  await archive.getByRole("button", { name: "load audio" }).click();
  await expect(
    archive.getByText("audio expired", { exact: true }),
  ).toBeVisible();
  await expect(archive.getByRole("link")).toHaveCount(0);
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

test("retained recordings remain accessible without connecting and keep epoch offsets", async ({
  page,
}) => {
  await page.goto("/?unpaired");
  await page.getByRole("button", { name: "recordings", exact: true }).click();
  await expect(
    page.getByRole("combobox", { name: "recording", exact: true }),
  ).toBeEnabled();
  await page.getByRole("button", { name: "load history", exact: true }).click();
  await expect(
    page.getByRole("img", { name: "display:fixture", exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("previous connection", { exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "load audio", exact: true }).click();
  await expect(page.getByRole("link", { name: /audio:fixture/ })).toBeVisible();
  await page
    .getByRole("combobox", { name: "recording", exact: true })
    .selectOption("00000000-0000-4000-8000-000000000012");
  await expect(
    page.getByText("previous connection", { exact: true }),
  ).not.toBeVisible();
  await page.getByRole("button", { name: "load history", exact: true }).click();
  await expect(
    page.getByText("older connection", { exact: true }),
  ).toBeVisible();
  const calls = await page.evaluate(() => (window as any).fixture.calls);
  const last = calls
    .filter((call: any) => call.name === "archive-images")
    .at(-1);
  expect(last.args.at_ms).toBe(1_199_400);
  expect(last.args.binding.epoch).toBe("00000000-0000-4000-8000-000000000012");
  expect(
    calls.some((call: any) =>
      ["connect", "observe", "configure"].includes(call.name),
    ),
  ).toBe(false);
  await page.evaluate(() => {
    (window as any).fixture.archiveExpired = true;
  });
  await page.getByRole("button", { name: "load history", exact: true }).click();
  await expect(
    page.getByRole("img", { name: "display:fixture", exact: true }),
  ).not.toBeVisible();
  await expect(
    page.getByText("transcripts expired", { exact: true }),
  ).toBeVisible();
  for (const width of [720, 1024, 1440]) {
    await page.setViewportSize({ width, height: 900 });
    expect(
      await page.evaluate(() => document.documentElement.scrollWidth),
    ).toBeLessThanOrEqual(width);
    await expect(
      page.getByRole("button", { name: "load history", exact: true }),
    ).toBeVisible();
  }
});

test("a pending recording read cannot replace a later epoch selection", async ({
  page,
}) => {
  await page.goto("/?unpaired");
  await page.getByRole("button", { name: "recordings", exact: true }).click();
  await page.evaluate(() => {
    (window as any).fixture.archiveDelay = true;
  });
  await page.getByRole("button", { name: "load history", exact: true }).click();
  await expect
    .poll(() =>
      page.evaluate(() =>
        (window as any).fixture.calls.some(
          (call: any) => call.name === "archive-images",
        ),
      ),
    )
    .toBe(true);
  await page
    .getByRole("combobox", { name: "recording", exact: true })
    .selectOption("00000000-0000-4000-8000-000000000012");
  await page.evaluate(() => {
    (window as any).fixture.archiveDelay = false;
  });
  await page.getByRole("button", { name: "load history", exact: true }).click();
  await expect(
    page.getByText("older connection", { exact: true }),
  ).toBeVisible();
  await page.waitForTimeout(600);
  await expect(
    page.getByText("older connection", { exact: true }),
  ).toBeVisible();
  await expect(
    page.getByText("previous connection", { exact: true }),
  ).not.toBeVisible();
});

test("a disconnected session can reconnect its pinned address or be explicitly stopped", async ({
  page,
}) => {
  await page.goto("/");
  await expect(
    page.getByRole("img", { name: "fixture display", exact: true }),
  ).toBeVisible();
  await page.evaluate(() => {
    const state = (window as any).fixture;
    state.disconnected = true;
    state.cursor++;
  });
  await page.getByRole("button", { name: "reconnect", exact: true }).click();
  await expect
    .poll(() =>
      page.evaluate(
        () =>
          (window as any).fixture.calls
            .filter((call: any) => call.name === "connect")
            .at(-1)?.args,
      ),
    )
    .toEqual({ address: "127.0.0.1:45810", peer: "a".repeat(64) });
  await expect(page.locator(".connection-state")).toHaveText("connected");
  await page.evaluate(() => {
    const state = (window as any).fixture;
    state.disconnected = true;
    state.cursor++;
  });
  await expect(page.locator(".connection-state")).toHaveText("disconnected");
  await page.getByRole("button", { name: "stop", exact: true }).click();
  await expect(page.locator(".connection-state")).toHaveText("stopped");
});

test("recovery failures remain visible in a live session and can be dismissed", async ({
  page,
}) => {
  await page.goto("/");
  await expect(
    page.getByRole("img", { name: "fixture display", exact: true }),
  ).toBeVisible();
  await page.evaluate(() => {
    (window as any).fixture.recovery = [
      {
        operation: "archive",
        source: null,
        message: "archive storage is unavailable",
      },
    ];
  });
  await expect(page.getByRole("status")).toContainText(
    "archive storage is unavailable",
  );
  await expect(
    page.getByRole("img", { name: "fixture display", exact: true }),
  ).toBeVisible();
  await page.getByRole("button", { name: "dismiss", exact: true }).click();
  await expect(page.getByRole("status")).not.toBeVisible();
});
