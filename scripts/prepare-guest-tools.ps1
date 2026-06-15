param(
    [string]$OutputDir = "target/release/agentdp-guest-tools"
)

$ErrorActionPreference = "Stop"

function Copy-GuestTools {
    param(
        [string]$SourceDir,
        [string]$DestinationDir
    )

    New-Item -ItemType Directory -Force -Path $DestinationDir | Out-Null
    foreach ($name in @("guestd", "guestctl")) {
        $source = Join-Path $SourceDir $name
        if (!(Test-Path -LiteralPath $source -PathType Leaf)) {
            throw "Linux guest tool '$name' was not found at '$source'. Set AGENTDP_GUEST_TOOL_DIR to a directory containing extensionless Linux guest tools, or install WSL with Rust/Cargo available."
        }
        Copy-Item -LiteralPath $source -Destination (Join-Path $DestinationDir $name) -Force
    }
}

if ($env:AGENTDP_GUEST_TOOL_DIR) {
    Copy-GuestTools -SourceDir $env:AGENTDP_GUEST_TOOL_DIR -DestinationDir $OutputDir
    exit 0
}

if (!(Get-Command wsl.exe -ErrorAction SilentlyContinue)) {
    throw "Windows install needs extensionless Linux guest tools. Install WSL with Rust/Cargo available, or set AGENTDP_GUEST_TOOL_DIR to a directory containing guestd and guestctl."
}

$workspace = (Get-Location).Path
$wslInputPath = $workspace -replace '\\', '/'
$wslWorkspace = (& wsl.exe wslpath -a $wslInputPath).Trim()
if (!$wslWorkspace) {
    throw "failed to translate workspace path '$workspace' for WSL"
}
$quotedWorkspace = "'" + ($wslWorkspace -replace "'", "'\''") + "'"
$command = "cd $quotedWorkspace && rustup target add x86_64-unknown-linux-musl && CARGO_TARGET_DIR=target/guest-linux-musl cargo build --release --target x86_64-unknown-linux-musl -p agentdp-guest"
& wsl.exe bash -lc $command
if ($LASTEXITCODE -ne 0) {
    throw "failed to build Linux guest tools through WSL"
}

Copy-GuestTools -SourceDir "target/guest-linux-musl/x86_64-unknown-linux-musl/release" -DestinationDir $OutputDir
