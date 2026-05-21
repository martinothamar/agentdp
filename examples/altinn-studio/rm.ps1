param(
    [string[]] $Instances = @("v1-0", "v1-1", "v1-2", "v1-3", "v1-4")
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$manifest = Join-Path $scriptDir "agent.yaml"

foreach ($instance in $Instances) {
    Write-Host "Removing $instance"
    agentctl rm -f $manifest $instance
}
