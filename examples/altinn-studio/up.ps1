param(
    [string[]] $Instances = @("pr-0", "pr-1", "pr-2", "pr-3", "pr-4")
)

$ErrorActionPreference = "Stop"

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$manifest = Join-Path $scriptDir "agent.yaml"

foreach ($instance in $Instances) {
    Write-Host "Starting $instance"
    agentctl up -f $manifest $instance
}
