#Requires -RunAsAdministrator

param(
    [ValidateRange(1, 65535)]
    [int] $Port = 4090
)

$ErrorActionPreference = "Stop"

$lanAddress = Get-NetIPConfiguration |
    Where-Object {
        $_.IPv4Address -and
        $_.NetAdapter.Status -eq "Up" -and
        $_.InterfaceAlias -notmatch "WSL|Loopback|Tailscale|Docker|vEthernet"
    } |
    ForEach-Object { $_.IPv4Address.IPAddress } |
    Where-Object {
        $_ -notlike "169.254.*" -and
        $_ -ne "127.0.0.1"
    } |
    Select-Object -First 1

if (-not $lanAddress) {
    throw "Could not find an active LAN IPv4 address."
}

$ruleName = "agentdp altinn-studio code-server $Port LAN"

netsh interface portproxy delete v4tov4 listenaddress=$lanAddress listenport=$Port | Out-Null
netsh interface portproxy add v4tov4 listenaddress=$lanAddress listenport=$Port connectaddress=127.0.0.1 connectport=$Port | Out-Null

$existingRule = Get-NetFirewallRule -DisplayName $ruleName -ErrorAction SilentlyContinue
if ($existingRule) {
    Remove-NetFirewallRule -DisplayName $ruleName
}

New-NetFirewallRule `
    -DisplayName $ruleName `
    -Direction Inbound `
    -Action Allow `
    -Protocol TCP `
    -LocalAddress $lanAddress `
    -LocalPort $Port | Out-Null

Write-Host "Forwarded http://$lanAddress`:$Port -> http://127.0.0.1`:$Port"
Write-Host "Open from Windows: http://desktop-win`:$Port"
Write-Host "Open from another LAN machine: http://$lanAddress`:$Port"
