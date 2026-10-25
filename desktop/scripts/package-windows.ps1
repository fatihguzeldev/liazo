param(
    [ValidateSet('baseline', 'avx2')][string]$Cpu = 'baseline',
    [ValidateSet('cpu', 'vulkan')][string]$Backend = 'cpu',
    [string]$BuildRoot = ''
)
$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest
if (!$IsWindows) { throw 'Windows packaging requires a native Windows build' }

function Get-SourceStatus([string]$Phase) {
    $changes = @(& git status --porcelain --untracked-files=normal)
    if ($LASTEXITCODE -ne 0) { throw 'source status unavailable' }
    Write-Host "source status ($Phase):"
    if ($changes.Count) {
        $changes | ForEach-Object { Write-Host $_ }
    } else { Write-Host 'clean' }
    return $changes
}

function Get-VerifiedDownload([string]$Uri, [string]$Path, [string]$Sha256) {
    if (!(Test-Path -LiteralPath $Path)) {
        $partial = "$Path.part"
        if (Test-Path -LiteralPath $partial) { throw "incomplete dependency download: $partial" }
        Invoke-WebRequest -Uri $Uri -UserAgent 'Liazo-build/0.1' -OutFile $partial -TimeoutSec 300
        if ((Get-FileHash $partial -Algorithm SHA256).Hash.ToLowerInvariant() -ne $Sha256) {
            throw 'dependency download checksum mismatch'
        }
        Move-Item -LiteralPath $partial -Destination $Path
    }
    if ((Get-FileHash $Path -Algorithm SHA256).Hash.ToLowerInvariant() -ne $Sha256) {
        throw 'cached dependency checksum mismatch'
    }
}

$root = (Resolve-Path (Join-Path $PSScriptRoot '../..')).Path
$target = 'x86_64-pc-windows-msvc'
$variant = if ($Backend -eq 'cpu') { $Cpu } else { "$Cpu-$Backend" }
# Vulkan's nested CMake compiler checks exceed MSVC's path limit with a descriptive build directory.
$buildDirectory = if ($Backend -eq 'vulkan') {
    if ($Cpu -eq 'baseline') { 'target/vb' } else { 'target/va' }
} else { "target/packages/windows-$variant" }
$targetDir = if ($BuildRoot) { [IO.Path]::GetFullPath($BuildRoot) } else { Join-Path $root $buildDirectory }
$output = Join-Path $root "target/distribution/$target-$variant"
$stage = Join-Path $root 'desktop/src-tauri/binaries'
$runtime = Join-Path $stage 'runtime'
$featureArgs = @(if ($Backend -eq 'vulkan') { '--features'; 'vulkan' })
$vulkanVersion = '1.4.363.0'
$vulkanRuntime = $null
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
if ($Backend -eq 'vulkan') { $settings.CMAKE_GENERATOR = 'Ninja' }
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
    $sourceChangesBefore = @(Get-SourceStatus 'before cargo')
    foreach ($entry in $settings.GetEnumerator()) {
        $changedEnvironment[$entry.Key] = [Environment]::GetEnvironmentVariable($entry.Key, 'Process')
        [Environment]::SetEnvironmentVariable($entry.Key, $entry.Value, 'Process')
    }
    $metadata = & cargo metadata --locked --no-deps --format-version 1 | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0) { throw 'cargo metadata failed' }
    $version = ($metadata.packages | Where-Object name -eq 'liazo').version
    $vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio/Installer/vswhere.exe'
    $visualStudio = & $vswhere -latest -products '*' -requires Microsoft.VisualStudio.Component.VC.Tools.x86.x64 -property installationPath
    if ($LASTEXITCODE -ne 0 -or !$visualStudio) { throw 'Visual C++ build tools are unavailable' }
    if ($Backend -eq 'vulkan') {
        $devCmd = Join-Path $visualStudio 'Common7/Tools/VsDevCmd.bat'
        if (!(Test-Path -LiteralPath $devCmd)) { throw 'Visual C++ developer environment is unavailable' }
        $devEnvironment = & $env:ComSpec /d /s /c "call `"$devCmd`" -no_logo -arch=x64 -host_arch=x64 >nul && set" 2>&1
        if ($LASTEXITCODE -ne 0) { throw 'Visual C++ developer environment setup failed' }
        $compilerVariables = @(
            'PATH', 'INCLUDE', 'LIB', 'LIBPATH', 'VCINSTALLDIR', 'VCToolsInstallDir', 'VCToolsVersion',
            'VSINSTALLDIR', 'VisualStudioVersion', 'WindowsSdkDir', 'WindowsSDKVersion',
            'WindowsSdkBinPath', 'WindowsSdkVerBinPath', 'UniversalCRTSdkDir', 'UCRTVersion',
            'VSCMD_ARG_HOST_ARCH', 'VSCMD_ARG_TGT_ARCH', 'VSCMD_VER'
        )
        foreach ($line in $devEnvironment) {
            $text = $line.ToString()
            $separator = $text.IndexOf('=')
            if ($separator -le 0) { continue }
            $name = $text.Substring(0, $separator)
            if ($name -notin $compilerVariables) { continue }
            if (!$changedEnvironment.ContainsKey($name)) {
                $changedEnvironment[$name] = [Environment]::GetEnvironmentVariable($name, 'Process')
            }
            [Environment]::SetEnvironmentVariable($name, $text.Substring($separator + 1), 'Process')
        }
        Remove-Variable devEnvironment
        if (!$env:VCToolsInstallDir) { throw 'MSVC toolchain directory was not configured' }
        $compilerDirectory = (Resolve-Path (Join-Path $env:VCToolsInstallDir 'bin/Hostx64/x64')).Path
        $compilerPath = [IO.Path]::GetFullPath((Join-Path $compilerDirectory 'cl.exe'))
        $linkerPath = [IO.Path]::GetFullPath((Join-Path $compilerDirectory 'link.exe'))
        if (!(Test-Path -LiteralPath $compilerPath) -or !(Test-Path -LiteralPath $linkerPath)) {
            throw 'native x64 MSVC compiler or linker is unavailable'
        }
        if (!$changedEnvironment.ContainsKey('PATH')) {
            $changedEnvironment['PATH'] = [Environment]::GetEnvironmentVariable('PATH', 'Process')
        }
        [Environment]::SetEnvironmentVariable('PATH', "$compilerDirectory;$env:PATH", 'Process')
        $compiler = Get-Command cl.exe -CommandType Application -ErrorAction Stop | Select-Object -First 1
        $linker = Get-Command link.exe -CommandType Application -ErrorAction Stop | Select-Object -First 1
        $tools = (Resolve-Path (Join-Path $visualStudio 'VC/Tools/MSVC')).Path
        $compilerMatches = [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($compiler.Source), $compilerPath)
        $linkerMatches = [StringComparer]::OrdinalIgnoreCase.Equals([IO.Path]::GetFullPath($linker.Source), $linkerPath)
        Write-Host (@{
            compiler = $compiler.Source; linker = $linker.Source
            expectedCompiler = $compilerPath; expectedLinker = $linkerPath
            hostArch = $env:VSCMD_ARG_HOST_ARCH; targetArch = $env:VSCMD_ARG_TGT_ARCH
        } | ConvertTo-Json -Compress)
        if (!$compilerDirectory.StartsWith($tools + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase) -or
            !$compilerMatches -or !$linkerMatches -or
            ([string]$env:VSCMD_ARG_TGT_ARCH).Trim() -ne 'x64' -or ([string]$env:VSCMD_ARG_HOST_ARCH).Trim() -ne 'x64') {
            throw 'expected the native x64 MSVC compiler and linker'
        }
        $includeDirectories = @($env:INCLUDE -split ';' | Where-Object { $_ })
        foreach ($headerName in @('vcruntime.h', 'corecrt.h', 'windows.h', 'vector')) {
            if (!@($includeDirectories | Where-Object { Test-Path -LiteralPath (Join-Path $_ $headerName) }).Count) {
                throw "MSVC or Windows SDK header is unavailable: $headerName"
            }
        }
        Write-Host "Vulkan host compiler: $($compiler.Source)"
        $ninja = Get-Command ninja.exe -CommandType Application -ErrorAction Stop | Select-Object -First 1
        & $ninja.Source --version
        if ($LASTEXITCODE -ne 0) { throw 'Ninja failed to start' }
        Write-Host "Vulkan CMake generator: Ninja ($($ninja.Source))"
        if (!$env:VULKAN_SDK -or
            !(Test-Path (Join-Path $env:VULKAN_SDK 'Include/vulkan/vulkan_core.h')) -or
            !(Test-Path (Join-Path $env:VULKAN_SDK 'Lib/vulkan-1.lib')) -or
            !(Test-Path (Join-Path $env:VULKAN_SDK 'Bin/glslc.exe'))) {
            throw 'Vulkan packaging requires the Vulkan SDK headers, import library, and glslc'
        }
        $header = Get-Content (Join-Path $env:VULKAN_SDK 'Include/vulkan/vulkan_core.h') -Raw
        if ($header -notmatch '(?m)^#define VK_HEADER_VERSION\s+363\s*$') {
            throw "Vulkan packaging requires SDK $vulkanVersion"
        }
        & (Join-Path $env:VULKAN_SDK 'Bin/glslc.exe') --version
        if ($LASTEXITCODE -ne 0) { throw 'Vulkan shader compiler failed to start' }
    }
    & cargo build --locked --release --target $target -p liazo @featureArgs
    if ($LASTEXITCODE -ne 0) { throw 'companion build failed' }
    Get-SourceStatus 'after companion' | Out-Null
    $release = Join-Path $targetDir "$target/release"
    New-Item -ItemType Directory -Path $stage, $runtime, $output -Force | Out-Null
    Copy-Item (Join-Path $release 'liazo.exe') (Join-Path $stage "liazo-$target.exe") -Force

    $redist = Get-ChildItem (Join-Path $visualStudio 'VC/Redist/MSVC/*/x64/Microsoft.VC*.CRT') -Directory |
        Sort-Object FullName -Descending | Select-Object -First 1
    if (!$redist) { throw 'Visual C++ redistributable DLLs are unavailable' }
    Get-ChildItem $runtime -Filter '*.dll' | Remove-Item
    foreach ($name in @('VulkanRT-License.txt', 'Vulkan-Loader-LICENSE.txt')) {
        $previousNotice = Join-Path $runtime $name
        if (Test-Path -LiteralPath $previousNotice) { Remove-Item -LiteralPath $previousNotice }
    }
    Copy-Item (Join-Path $redist.FullName '*.dll') $runtime
    if ($Backend -eq 'vulkan') {
        $cache = Join-Path $root "target/dependencies/vulkan-$vulkanVersion"
        New-Item -ItemType Directory -Path $cache -Force | Out-Null
        $archive = Join-Path $cache "VulkanRT-X64-$vulkanVersion-Components.zip"
        $archiveHash = 'a25a927aa8b9f0371048f1861cf88ac3b9bc9b1fb332c42d897c8ab32695769a'
        Get-VerifiedDownload "https://sdk.lunarg.com/sdk/download/$vulkanVersion/windows/$(Split-Path $archive -Leaf)" $archive $archiveHash
        $license = Join-Path $cache 'Vulkan-Loader-LICENSE.txt'
        $licenseHash = '43c0a37e6a0fa7ff3c843b3ec5a4fac84b712558ddac103fbd4c1649662a9ece'
        Get-VerifiedDownload "https://raw.githubusercontent.com/KhronosGroup/Vulkan-Loader/vulkan-sdk-$vulkanVersion/LICENSE.txt" $license $licenseHash
        Expand-Archive -LiteralPath $archive -DestinationPath $cache -Force
        $components = Join-Path $cache "VulkanRT-X64-$vulkanVersion-Components"
        $loader = Join-Path $components 'x64/vulkan-1.dll'
        $loaderHash = 'e1fcfc9489beefa6a6d9d11d6c7517f6a2b7f16e0d3fb8f4103bee0210f314a3'
        if ((Get-FileHash $loader -Algorithm SHA256).Hash.ToLowerInvariant() -ne $loaderHash) {
            throw 'Vulkan loader checksum mismatch'
        }
        $notice = Join-Path $components 'VulkanRT-License.txt'
        $noticeHash = '404deb98dabf3e54bf9070d59676b31d69a3da2f9f6e56bdf09ad315c04c718c'
        if ((Get-FileHash $notice -Algorithm SHA256).Hash.ToLowerInvariant() -ne $noticeHash) {
            throw 'Vulkan runtime notice checksum mismatch'
        }
        Copy-Item $loader, $notice, $license $runtime
        $vulkanRuntime = @{
            version = $vulkanVersion; archiveSha256 = $archiveHash; loaderSha256 = $loaderHash
            noticeSha256 = $noticeHash; licenseSha256 = $licenseHash
        }
    }

    Push-Location (Join-Path $root 'desktop')
    try {
        & npm run tauri -- build --target $target --config src-tauri/bundle.windows.json --bundles nsis @featureArgs
        if ($LASTEXITCODE -ne 0) { throw 'desktop installer build failed' }
    } finally { Pop-Location }
    Get-SourceStatus 'after desktop bundle' | Out-Null

    $portable = Join-Path $output 'portable'
    if (Test-Path $portable) { Remove-Item $portable -Recurse -Force }
    New-Item -ItemType Directory -Path $portable | Out-Null
    Copy-Item (Join-Path $release 'liazo.exe') $portable
    Copy-Item (Join-Path $release 'liazo-desktop.exe') $portable
    Copy-Item (Join-Path $runtime '*.dll') $portable
    Get-ChildItem $runtime -Filter '*License*.txt' | Copy-Item -Destination $portable
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
    if ($Backend -eq 'vulkan') {
        Copy-Item (Join-Path $runtime 'vulkan-1.dll') (Join-Path $release 'deps') -Force
    }
    $previousCli = $env:LIAZO_TEST_CLI
    $env:LIAZO_TEST_CLI = Join-Path $portable 'liazo.exe'
    try {
        & cargo test --locked --release --target $target -p liazo --test mcp_stdio @featureArgs
        if ($LASTEXITCODE -ne 0) { throw 'packaged MCP smoke test failed' }
    } finally { $env:LIAZO_TEST_CLI = $previousCli }

    $source = & git rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw 'source identity unavailable' }
    $sourceChanges = @(Get-SourceStatus 'after packaged smoke')
    $sourceDirty = [bool]$sourceChanges
    @{
        version = $version
        source = $source
        sourceDirty = $sourceDirty
        sourceChangesBefore = $sourceChangesBefore
        sourceChanges = $sourceChanges
        target = $target
        cpu = $Cpu
        backend = $Backend
        cargoFeatures = @(if ($Backend -eq 'vulkan') { 'vulkan' })
        vulkanRuntime = $vulkanRuntime
        vulkanSdk = $(if ($Backend -eq 'vulkan') { @{ headerVersion = 363; archiveSha256 = $env:LIAZO_VULKAN_SDK_SHA256 } } else { $null })
        runtimeDependencies = @(Get-ChildItem $runtime -File | Sort-Object Name | ForEach-Object {
            @{ name = $_.Name; bytes = $_.Length; sha256 = (Get-FileHash $_.FullName -Algorithm SHA256).Hash.ToLowerInvariant() }
        })
        cpuRequirements = @(if ($Cpu -eq 'avx2') { 'SSE4.2'; 'AVX'; 'AVX2'; 'BMI2'; 'FMA'; 'F16C' } else { 'x86-64 baseline' })
        webview2 = 'installer embeds offline setup; portable desktop requires an installed WebView2 runtime'
        signing = 'unsigned'
    } | ConvertTo-Json -Depth 6 | Set-Content (Join-Path $output 'build.json') -Encoding utf8
    Copy-Item (Join-Path $output 'build.json') $portable
    Compress-Archive -Path (Join-Path $portable '*') -DestinationPath (Join-Path $output "liazo-$version-$target-$variant-portable.zip") -Force
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
