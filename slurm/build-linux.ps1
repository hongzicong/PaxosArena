[CmdletBinding()]
param(
    [string]$Output = "consensusarena-linux-amd64"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
$outputPath = if ([System.IO.Path]::IsPathRooted($Output)) {
    [System.IO.Path]::GetFullPath($Output)
}
else {
    [System.IO.Path]::GetFullPath((Join-Path $repoRoot $Output))
}

$previousCgo = [Environment]::GetEnvironmentVariable("CGO_ENABLED", "Process")
$previousOs = [Environment]::GetEnvironmentVariable("GOOS", "Process")
$previousArch = [Environment]::GetEnvironmentVariable("GOARCH", "Process")
$previousCache = [Environment]::GetEnvironmentVariable("GOCACHE", "Process")

try {
    $env:CGO_ENABLED = "0"
    $env:GOOS = "linux"
    $env:GOARCH = "amd64"
    $env:GOCACHE = Join-Path $repoRoot ".gocache"
    Push-Location $repoRoot
    try {
        & go build -trimpath -o $outputPath .
        if ($LASTEXITCODE -ne 0) {
            throw "go build failed with exit code $LASTEXITCODE"
        }
    }
    finally {
        Pop-Location
    }
}
finally {
    [Environment]::SetEnvironmentVariable("CGO_ENABLED", $previousCgo, "Process")
    [Environment]::SetEnvironmentVariable("GOOS", $previousOs, "Process")
    [Environment]::SetEnvironmentVariable("GOARCH", $previousArch, "Process")
    [Environment]::SetEnvironmentVariable("GOCACHE", $previousCache, "Process")
}

Get-Item -LiteralPath $outputPath | Select-Object FullName, Length, LastWriteTime
