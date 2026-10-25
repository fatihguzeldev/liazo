param([ValidateSet('baseline', 'avx2')][string]$Cpu = 'baseline')
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (!$IsWindows) { throw 'Windows packaging requires a native Windows build' }

$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$target = 'x86_64-pc-windows-msvc'
$targetDir = Join-Path $root "target/packages/windows-$Cpu"
$output = Join-Path $root "target/distribution/$target-$Cpu"
$stage = Join-Path $root 'desktop/src-tauri/binaries'
$runtime = Join-Path $stage 'runtime'
$changedEnvironment = @{}
$settings = @{
    CARGO_TARGET_DIR = $targetDir
    GGML_NATIVE = 'OFF'
    GGML_SSE42 = 'OFF'
    GGML_AVX = 'OFF'
    GGML_AVX2 = 'OFF'
    GGML_BMI2 = 'OFF'
    GGML_AVX512 = 'OFF'
    GGML_AVX_VNNI = 'OFF'
    GGML_FMA = 'OFF'
    GGML_F16C = 'OFF'
}
if ($Cpu -eq 'avx2') {
    $settings.GGML_SSE42 = 'ON'
    $settings.GGML_AVX = 'ON'
    $settings.GGML_AVX2 = 'ON'
    $settings.GGML_BMI2 = 'ON'
    $settings.GGML_FMA = 'ON'
    $settings.GGML_F16C = 'ON'
}

Push-Location $root
try {
    foreach ($entry in $settings.GetEnumerator()) {
        $changedEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, 'Process')
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    $metadata = & cargo metadata --locked --no-deps --format-version 1 | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'cargo metadata failed' }
    $version = ($metadata.packages | Where-Object name -eq 'liazo').version
    & cargo build --locked --release --target $target -p liazo
    if ($LASTEXITCODE -ne 0) { throw 'companion build failed' }
    $release = Join-Path $targetDir "$target/release"
    New-Item -ItemType Directory -Path $stage, $runtime, $output -Force | Out-Null
    Copy-Item (Join-Path $release 'liazo.exe') (Join-Path $stage "liazo-$target.exe") -Force

    $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
    $visualStudio = & $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
    if ($LASTEXITCODE -ne 0 -or !$visualStudio) { throw 'Visual C++ build tools are unavailable' }
    $redist = Get-ChildItem (Join-Path $visualStudio 'VC/Redist/MSVC/*/x64/Microsoft.VC*.CRT') -Directory |
        Sort-Object FullName -Descending | Select-Object -First 1
    if (!$redist) { throw 'Visual C++ redistributable DLLs are unavailable' }
    Get-ChildItem $runtime -Filter '*.dll' | Remove-Item
    Copy-Item (Join-Path $redist.FullName '*.dll') $runtime

    Push-Location (Join-Path $root 'desktop')
    try {
        & npm run tauri -- build --target $target --config src-tauri/bundle.windows.json --bundles nsis
        if ($LASTEXITCODE -ne 0) { throw 'desktop installer build failed' }
    } finally { Pop-Location }

    $portable = Join-Path $output 'portable'
    if (Test-Path $portable) { Remove-Item $portable -Recurse -Force }
    New-Item -ItemType Directory -Path $portable | Out-Null
    Copy-Item (Join-Path $release 'liazo.exe') $portable
    Copy-Item (Join-Path $release 'liazo-desktop.exe') $portable
    Copy-Item (Join-Path $runtime '*.dll') $portable
    $dumpbin = Get-ChildItem (Join-Path $visualStudio 'VC/Tools/MSVC/*/bin/Hostx64/x64/dumpbin.exe') |
        Sort-Object FullName -Descending | Select-Object -First 1
    if (!$dumpbin) { throw 'dumpbin is unavailable' }
    foreach ($binary in Get-ChildItem $portable -Include '*.exe', '*.dll' -Recurse) {
        $dependencies = & $dumpbin.FullName /DEPENDENTS $binary.FullName
        if ($LASTEXITCODE -ne 0) { throw "cannot inspect $($binary.Name)" }
        foreach ($line in $dependencies) {
            if ($line -match '^\s+([A-Za-z0-9_.-]+\.dll)\s*$') {
                $dependency = $Matches[1]
                if (!(Test-Path (Join-Path $portable $dependency)) -and
                    !(Test-Path (Join-Path $env:SystemRoot "System32/$dependency")) -and
                    !$dependency.StartsWith('api-ms-win-', [StringComparison]::OrdinalIgnoreCase)) {
                    throw "unbundled dependency: $dependency ($($binary.Name))"
                }
            }
        }
    }
    & (Join-Path $portable 'liazo.exe') --help | Out-Null
    if ($LASTEXITCODE -ne 0) { throw 'packaged CLI failed to start' }
    $previousCli = $env:LIAZO_TEST_CLI
    $env:LIAZO_TEST_CLI = Join-Path $portable 'liazo.exe'
    try {
        & cargo test --locked --target $target -p liazo --test mcp_stdio
        if ($LASTEXITCODE -ne 0) { throw 'packaged MCP smoke test failed' }
    } finally { $env:LIAZO_TEST_CLI = $previousCli }

    $source = & git rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw 'source identity unavailable' }
    $sourceDirty = [bool](& git status --porcelain --untracked-files=normal)
    if ($LASTEXITCODE -ne 0) { throw 'source status unavailable' }
    @{
        version = $version
        source = $source
        sourceDirty = $sourceDirty
        target = $target
        cpu = $Cpu
        cpuRequirements = @(if ($Cpu -eq 'avx2') { 'SSE4.2'; 'AVX'; 'AVX2'; 'BMI2'; 'FMA'; 'F16C' } else { 'x86-64 baseline' })
        webview2 = 'installer embeds offline setup; portable desktop requires an installed WebView2 runtime'
        signing = 'unsigned'
    } | ConvertTo-Json | Set-Content (Join-Path $output 'build.json') -Encoding utf8
    Copy-Item (Join-Path $output 'build.json') $portable
    Compress-Archive -Path (Join-Path $portable '*') -DestinationPath (Join-Path $output "liazo-$version-$target-$Cpu-portable.zip") -Force
    Copy-Item (Join-Path $release 'bundle/nsis/*-setup.exe') $output -Force
    Get-ChildItem $output -File | Where-Object Extension -in '.exe', '.zip' | ForEach-Object {
        $hash = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
        "$hash  $($_.Name)" | Set-Content "$($_.FullName).sha256" -Encoding ascii
    }
    Write-Output $output
} finally {
    foreach ($entry in $changedEnvironment.GetEnumerator()) {
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    Pop-Location
}
