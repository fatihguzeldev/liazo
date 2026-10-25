#!/bin/bash
set -euo pipefail

if [[ "$(uname -s)" != Darwin ]]; then
  echo 'macOS packaging requires a native Mac build' >&2
  exit 1
fi
root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
metadata="$(cargo metadata --locked --no-deps --format-version 1)"
target_dir="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["target_directory"])' <<< "$metadata")"
version="$(python3 -c 'import json,sys; print(next(p["version"] for p in json.load(sys.stdin)["packages"] if p["name"] == "liazo"))' <<< "$metadata")"
architecture="$(uname -m)"
case "$architecture" in
  arm64) target=aarch64-apple-darwin ;;
  x86_64) target=x86_64-apple-darwin ;;
  *) echo "unsupported Mac architecture: $architecture" >&2; exit 1 ;;
esac

export MACOSX_DEPLOYMENT_TARGET=14.2
export GGML_NATIVE=OFF
if [[ "$architecture" == x86_64 ]]; then
  export GGML_SSE42=OFF GGML_AVX=OFF GGML_AVX2=OFF GGML_BMI2=OFF GGML_FMA=OFF GGML_F16C=OFF
fi
export RUSTFLAGS="${RUSTFLAGS:-} -C link-arg=-Wl,-rpath,@executable_path/../Frameworks"
cargo build --locked --release --target "$target" -p liazo

output="$target_dir/distribution/$target"
mkdir -p "$output"
staging="$(mktemp -d "$output/.package.XXXXXX")"
trap 'rm -rf "$staging"' EXIT
bundle="$staging/liazo.app"
mkdir -p "$bundle/Contents/MacOS" "$bundle/Contents/Frameworks"
mkdir -p "$bundle/Contents/Resources"
cp "$target_dir/$target/release/liazo" "$bundle/Contents/MacOS/liazo"
cp platform/macos/Info.plist "$bundle/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleVersion $version" "$bundle/Contents/Info.plist"
/usr/libexec/PlistBuddy -c "Set :CFBundleShortVersionString $version" "$bundle/Contents/Info.plist"
plutil -lint "$bundle/Contents/Info.plist"
xcrun swift-stdlib-tool --copy --scan-executable "$bundle/Contents/MacOS/liazo" \
  --platform macosx --destination "$bundle/Contents/Frameworks"

python3 - "$bundle" <<'PY'
import pathlib, subprocess, sys
bundle = pathlib.Path(sys.argv[1])
for executable in [bundle / "Contents/MacOS/liazo", *sorted((bundle / "Contents/Frameworks").glob("*.dylib"))]:
    lines = subprocess.check_output(["xcrun", "otool", "-L", str(executable)], text=True).splitlines()[1:]
    for line in lines:
        dependency = line.strip().split(" (", 1)[0]
        if dependency.startswith(("/System/Library/", "/usr/lib/")):
            continue
        if dependency.startswith("@rpath/"):
            name = dependency.removeprefix("@rpath/")
            if (bundle / "Contents/Frameworks" / name).is_file():
                continue
            if name.startswith("libswift") and pathlib.Path("/usr/lib/swift", name).is_file():
                continue
        raise SystemExit(f"unbundled runtime dependency: {dependency}")
PY

python3 - "$root" "$bundle/Contents/Resources/build.json" "$target" "$version" <<'PY'
import json, pathlib, subprocess, sys
root, output, target, version = sys.argv[1:]
source = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
dirty = bool(subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=normal"], cwd=root))
pathlib.Path(output).write_text(json.dumps({
    "version": version, "source": source, "sourceDirty": dirty, "target": target,
    "cpu": "portable", "minimumSystemVersion": "14.2", "signing": "ad-hoc",
}, indent=2) + "\n")
PY

# An ad-hoc signature validates this local bundle. It is not a Developer ID signature.
find "$bundle/Contents/Frameworks" -name '*.dylib' -type f -exec \
  codesign --force --sign - --timestamp=none {} \;
codesign --force --sign - --timestamp=none "$bundle"
codesign --verify --deep --strict "$bundle"
"$bundle/Contents/MacOS/liazo" --help >/dev/null

archive="$output/liazo-$version-$target.zip"
ditto -c -k --sequesterRsrc --keepParent "$bundle" "$staging/package.zip"
mv "$staging/package.zip" "$archive"
rm -rf "$output/liazo.app"
mv "$bundle" "$output/liazo.app"
cp "$output/liazo.app/Contents/Resources/build.json" "$output/build.json"
(cd "$output" && shasum -a 256 "$(basename "$archive")") > "$archive.sha256"
printf '%s\n' "$archive"
