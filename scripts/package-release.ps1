param(
    [Parameter(Mandatory = $true)][string]$Target,
    [Parameter(Mandatory = $true)][string]$Version
)

$ErrorActionPreference = "Stop"
$Name = "qcli-$Version-$Target"
$Stage = Join-Path "dist" $Name

if (Test-Path "dist") { Remove-Item "dist" -Recurse -Force }
New-Item -ItemType Directory -Path (Join-Path $Stage "completions") | Out-Null
New-Item -ItemType Directory -Path (Join-Path $Stage "docs") | Out-Null
New-Item -ItemType Directory -Path (Join-Path $Stage "deploy") | Out-Null
Copy-Item "target/$Target/release/qcli.exe" (Join-Path $Stage "qcli.exe")
Copy-Item "README.md" (Join-Path $Stage "README.md")
Copy-Item -Path @("LICENSE", "CHANGELOG.md") -Destination $Stage
Copy-Item -Path @("docs/connectivity-compatibility.md", "docs/enterprise-identity-and-transport.md", "docs/high-availability.md", "docs/operations.md", "docs/releasing.md", "docs/supported-platforms.md", "docs/unified-connectivity-release.md") -Destination (Join-Path $Stage "docs")
Copy-Item -Path @("deploy/kubernetes", "deploy/systemd") -Destination (Join-Path $Stage "deploy") -Recurse
(Get-Content "packaging/qcli.1") -replace "qcli 0\.1\.0", "qcli $Version" |
    Set-Content (Join-Path $Stage "qcli.1")
Copy-Item "packaging/completions/*" (Join-Path $Stage "completions")
Compress-Archive -Path $Stage -DestinationPath "dist/$Name.zip"
Remove-Item $Stage -Recurse -Force
