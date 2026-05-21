#Requires -RunAsAdministrator

param(
    [ValidateRange(1, 65535)]
    [int[]] $Ports = @(4090, 4091, 4092, 4093, 4094),

    [string] $ListenAddress
)

$ErrorActionPreference = "Stop"

$Ports = $Ports | Sort-Object -Unique
$hostName = hostname

if (-not $ListenAddress) {
    $ListenAddress = Get-NetIPConfiguration |
        Where-Object {
            $_.IPv4Address -and
            $_.IPv4DefaultGateway -and
            $_.NetAdapter.Status -eq "Up" -and
            $_.InterfaceAlias -notmatch "WSL|Loopback|Tailscale|Docker|vEthernet|VirtualBox|VMware|Host-Only"
        } |
        ForEach-Object { $_.IPv4Address.IPAddress } |
        Where-Object {
            $_ -notlike "169.254.*" -and
            $_ -ne "127.0.0.1"
        } |
        Select-Object -First 1
}

if (-not $ListenAddress) {
    throw "Could not find an active LAN IPv4 address. Pass -ListenAddress explicitly."
}

if ($ListenAddress -eq "0.0.0.0") {
    throw "Do not use 0.0.0.0 here. It also binds 127.0.0.1 and prevents QEMU from starting."
}

$existingPortProxy = netsh interface portproxy show v4tov4
foreach ($line in $existingPortProxy) {
    if ($line -match "^\s*(\d+\.\d+\.\d+\.\d+)\s+(\d+)\s+(\d+\.\d+\.\d+\.\d+)\s+(\d+)\s*$") {
        $existingAddress = $Matches[1]
        $existingPort = [int] $Matches[2]
        if ($Ports -contains $existingPort) {
            netsh interface portproxy delete v4tov4 listenaddress=$existingAddress listenport=$existingPort | Out-Null
        }
    }
}

foreach ($port in $Ports) {
    $ruleName = "agentdp altinn-studio code-server $port LAN"

    netsh interface portproxy add v4tov4 listenaddress=$ListenAddress listenport=$port connectaddress=127.0.0.1 connectport=$port | Out-Null

    Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue |
        Remove-NetFirewallRule

    New-NetFirewallRule `
        -DisplayName $ruleName `
        -Direction Inbound `
        -Action Allow `
        -Protocol TCP `
        -LocalPort $port | Out-Null

    Write-Host "Forwarded http://$ListenAddress`:$port -> http://127.0.0.1`:$port"
}

Write-Host ""
Write-Host "Open from another LAN machine:"
foreach ($port in $Ports) {
    Write-Host "  http://$hostName`:$port"
    Write-Host "  http://$ListenAddress`:$port"
}
Write-Host ""
Write-Host "If hostname resolution chooses the wrong adapter, use the host LAN IPv4 address instead."
