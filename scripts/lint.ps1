$ErrorActionPreference = 'Stop'

if (Get-Command go -ErrorAction SilentlyContinue) {
  go test ./...
} else {
  Write-Host "Go toolchain not available on PATH; skipping Go lint/test."
}

if (Test-Path client/admin-dashboard/package.json) {
  Push-Location client/admin-dashboard
  try {
    if (Get-Command npm -ErrorAction SilentlyContinue) {
      npm install
      npm run typecheck
      npm run build
    } else {
      Write-Host "npm not available on PATH; skipping frontend checks."
    }
  } finally {
    Pop-Location
  }
}
