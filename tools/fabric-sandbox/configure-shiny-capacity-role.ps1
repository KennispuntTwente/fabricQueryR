# One-time administrator setup. Preview by default; -Apply grants only the
# checked-in role on the existing F2. This script never starts capacity.
param(
    [string]$Repository = "KennispuntTwente/fabricQueryR",
    [string]$ResourceGroup = "fabric-rg",
    [string]$CapacityName = "rpackagecap",
    [switch]$Apply
)
$ErrorActionPreference = "Stop"

function Invoke-JsonCli {
    param([string]$Command, [string[]]$Arguments)
    $result = & $Command @Arguments 2>$null
    if ($LASTEXITCODE -ne 0) {
        throw "$Command failed during role configuration; no credential values are logged."
    }
    return ($result | ConvertFrom-Json)
}

$variablePath = "repos/$Repository/environments/fabric-integration/variables"
$subscription = (Invoke-JsonCli gh @("api", "$variablePath/AZURE_SUBSCRIPTION_ID")).value
$clientId = (Invoke-JsonCli gh @("api", "$variablePath/AZURE_CLIENT_ID")).value
$principal = Invoke-JsonCli az @("ad", "sp", "show", "--id", $clientId, "-o", "json")
$groupScope = "/subscriptions/$subscription/resourceGroups/$ResourceGroup"
$capacityScope = "$groupScope/providers/Microsoft.Fabric/capacities/$CapacityName"
$capacity = Invoke-JsonCli az @("resource", "show", "--ids", $capacityScope, "-o", "json")
if ($capacity.sku.name -ne "F2") { throw "The configured capacity is not F2." }

$rolePath = Join-Path $PSScriptRoot "../../infra/fabric/shiny-capacity-role.json"
$definition = Get-Content -LiteralPath $rolePath -Raw | ConvertFrom-Json
$definition.AssignableScopes = @($groupScope)
$roles = @(Invoke-JsonCli az @("role", "definition", "list", "--name", $definition.Name, "-o", "json"))
if ($roles.Count -gt 1) { throw "The role name is ambiguous." }
if ($roles.Count -eq 1) {
    $role = $roles[0]
    $permissions = @($role.permissions)
    if ($role.roleType -ne "CustomRole" -or $permissions.Count -ne 1 -or
        (($permissions[0].actions | Sort-Object) -join ',') -ne (($definition.Actions | Sort-Object) -join ',') -or
        @($permissions[0].notActions).Count -ne 0 -or
        @($permissions[0].dataActions).Count -ne 0 -or
        @($permissions[0].notDataActions).Count -ne 0 -or
        @($role.assignableScopes).Count -ne 1 -or $role.assignableScopes[0] -ne $groupScope) {
        throw "Existing role differs from the checked-in definition; refusing to change it."
    }
}

Write-Output "Role: $($definition.Name)"
Write-Output "Assignment scope: $capacityScope"
Write-Output "Permissions: $($definition.Actions -join ', ')"
Write-Output "Capacity state: $($capacity.properties.state)"
if (-not $Apply) {
    Write-Output "Preview only. Use -Apply to configure these permissions."
    return
}
if ($roles.Count -eq 0) {
    $roleFile = [System.IO.Path]::GetTempFileName()
    try {
        [System.IO.File]::WriteAllText($roleFile, ($definition | ConvertTo-Json -Depth 5))
        $role = Invoke-JsonCli az @("role", "definition", "create", "--role-definition", "@$roleFile", "-o", "json")
    } finally {
        Remove-Item -LiteralPath $roleFile
    }
}
$assignments = @(Invoke-JsonCli az @("role", "assignment", "list", "--assignee", $principal.id,
    "--scope", $capacityScope, "--role", $role.id, "-o", "json"))
if ($assignments.Count -eq 0) {
    $null = Invoke-JsonCli az @("role", "assignment", "create", "--assignee-object-id", $principal.id,
        "--assignee-principal-type", "ServicePrincipal", "--role", $role.id,
        "--scope", $capacityScope, "-o", "json")
}
Write-Output "CI capacity role configured. No capacity state changes requested."
