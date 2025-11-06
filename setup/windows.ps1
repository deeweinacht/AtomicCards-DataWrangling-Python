**setup/windows.ps1**
```powershell
# One-step setup for Windows users
$ErrorActionPreference = "Stop"

Write-Host "=== Setting up project environment (Windows) ==="

# 1) Ensure uv is available
if (-not (Get-Command uv -ErrorAction SilentlyContinue)) {
  Write-Host "Installing uv for current user..."
  py -3 -m pip install --user --upgrade uv
  $userBase = py -3 -m site --user-site | Split-Path -Parent
  $scripts  = Join-Path $userBase "Scripts"
  if (-not ($env:Path -split ';' | Where-Object { $_ -eq $scripts })) {
    $env:Path = "$env:Path;$scripts"
  }
}

# 2) Create/refresh venv and install from lockfile
uv venv .venv
uv sync

Write-Host "`nEnvironment ready. Activate with:"
Write-Host "   .\\.venv\\Scripts\\activate"