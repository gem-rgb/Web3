$ErrorActionPreference = 'Stop'

$protoRoot = Join-Path $PSScriptRoot '..\proto'
if (-not (Test-Path $protoRoot)) {
  Write-Host "Proto root not found at $protoRoot"
  exit 0
}

$buf = Get-Command buf -ErrorAction SilentlyContinue
$protoc = Get-Command protoc -ErrorAction SilentlyContinue

if ($buf) {
  Write-Host "buf detected, generating proto artifacts..."
  Push-Location $protoRoot
  try {
    & buf generate
    exit $LASTEXITCODE
  } finally {
    Pop-Location
  }
}

if ($protoc) {
  Write-Host "protoc detected, but no plugin chain is configured in this scaffold."
  Write-Host "Add buf or the Go protobuf plugins before enabling code generation."
  exit 0
}

Write-Host "No proto generator found. This repository keeps the current hand-written shared/proto compatibility layer for phase 1."
