#Requires -RunAsAdministrator

[CmdletBinding()]
param(
    [string]$FirewallRuleName = "OpenSSH-Server-Tailscale",
    [switch]$SkipFirewall,
    [switch]$SetPowerShellDefaultShell
)

$ErrorActionPreference = "Stop"

function Write-Step {
    param([string]$Message)

    Write-Host "==> $Message"
}

function Get-TailscaleIps {
    if (!(Get-Command tailscale.exe -ErrorAction SilentlyContinue)) {
        return @()
    }

    $ips = @()
    try {
        $ips += (& tailscale.exe ip -4 2>$null)
        $ips += (& tailscale.exe ip -6 2>$null)
    } catch {
        return @()
    }

    return $ips | Where-Object { $_ } | ForEach-Object { $_.Trim() }
}

function Get-TailscaleInterfaceAliases {
    Get-NetAdapter -ErrorAction SilentlyContinue |
        Where-Object {
            $_.Name -like "*Tailscale*" -or
            $_.InterfaceDescription -like "*Tailscale*"
        } |
        Select-Object -ExpandProperty Name -Unique
}

function Test-PasswordAuthenticationDisabled {
    $configPath = Join-Path $env:ProgramData "ssh\sshd_config"
    if (!(Test-Path -LiteralPath $configPath -PathType Leaf)) {
        return $false
    }

    $disabledLine = Get-Content -LiteralPath $configPath |
        Where-Object { $_ -match '^\s*PasswordAuthentication\s+no\s*(#.*)?$' } |
        Select-Object -First 1

    return [bool]$disabledLine
}

Write-Step "Installing Windows OpenSSH Server if needed"
$capability = Get-WindowsCapability -Online -Name "OpenSSH.Server~~~~0.0.1.0"
if ($capability.State -ne "Installed") {
    Add-WindowsCapability -Online -Name "OpenSSH.Server~~~~0.0.1.0" | Out-Null
}

Write-Step "Starting sshd and enabling it at boot"
if (!(Get-Service -Name sshd -ErrorAction SilentlyContinue)) {
    throw "The sshd service was not found after installing OpenSSH Server."
}
Set-Service -Name sshd -StartupType Automatic
Start-Service -Name sshd

if (!$SkipFirewall) {
    Write-Step "Creating firewall rule for SSH on the Tailscale interface"
    $tailscaleInterfaceAliases = @(Get-TailscaleInterfaceAliases)
    if ($tailscaleInterfaceAliases.Count -eq 0) {
        throw "No Tailscale network interface was found. Start Tailscale first, or rerun with -SkipFirewall and configure the firewall manually."
    }

    if (Get-NetFirewallRule -Name $FirewallRuleName -ErrorAction SilentlyContinue) {
        Remove-NetFirewallRule -Name $FirewallRuleName
    }

    New-NetFirewallRule `
        -Name $FirewallRuleName `
        -DisplayName "OpenSSH Server over Tailscale" `
        -Enabled True `
        -Direction Inbound `
        -Protocol TCP `
        -Action Allow `
        -LocalPort 22 `
        -InterfaceAlias $tailscaleInterfaceAliases | Out-Null
}

if ($SetPowerShellDefaultShell) {
    Write-Step "Setting PowerShell as the default OpenSSH shell"
    $defaultShell = "$env:SystemRoot\System32\WindowsPowerShell\v1.0\powershell.exe"
    New-Item -Path "HKLM:\SOFTWARE\OpenSSH" -Force | Out-Null
    New-ItemProperty `
        -Path "HKLM:\SOFTWARE\OpenSSH" `
        -Name DefaultShell `
        -Value $defaultShell `
        -PropertyType String `
        -Force | Out-Null
}

$tailscaleIps = Get-TailscaleIps
$hostName = $env:COMPUTERNAME
$userName = $env:USERNAME
$passwordAuthenticationDisabled = Test-PasswordAuthenticationDisabled

Write-Host ""
Write-Host "Remote SSH setup is enabled."
Write-Host "Use the normal Windows password for '$userName' when SSH prompts for it."
if ($passwordAuthenticationDisabled) {
    Write-Warning "PasswordAuthentication is explicitly disabled in sshd_config. Password login will not work until that is changed or SSH keys are configured."
}
Write-Host ""
Write-Host "From your laptop, test one of these:"
Write-Host "  ssh $userName@$hostName"
foreach ($ip in $tailscaleIps) {
    Write-Host "  ssh $userName@$ip"
}
Write-Host ""
Write-Host "VS Code Remote-SSH config example:"
Write-Host ""
Write-Host "Host $hostName"
Write-Host "  HostName $hostName"
Write-Host "  User $userName"
Write-Host ""
Write-Host "If MagicDNS is not enabled in Tailscale, use this instead:"
foreach ($ip in $tailscaleIps | Where-Object { $_ -match '^\d+\.' } | Select-Object -First 1) {
    Write-Host "  HostName $ip"
}
