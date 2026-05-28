$ErrorActionPreference = 'Stop'

if (Get-Command go -ErrorAction SilentlyContinue) {
  go test ./...
} else {
  Write-Host "Go toolchain not available on PATH; skipping workspace tests."
}

