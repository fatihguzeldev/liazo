import { execFileSync } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { compile } from "json-schema-to-typescript";

const root = fileURLToPath(new URL("../../", import.meta.url));
const schema = JSON.parse(
  execFileSync(
    "cargo",
    ["run", "--quiet", "-p", "liazo-host", "--example", "bindings"],
    { cwd: root, maxBuffer: 4 * 1024 * 1024, encoding: "utf8" },
  ),
);
const source = await compile(schema, "DesktopApi", {
  bannerComment: "/* generated from Rust; run npm run bindings */",
  additionalProperties: false,
  style: { printWidth: 80 },
});
await mkdir(new URL("../src/", import.meta.url), { recursive: true });
await writeFile(new URL("../src/bindings.ts", import.meta.url), source);
