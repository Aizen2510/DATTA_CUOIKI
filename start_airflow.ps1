$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

if (-not (Test-Path -LiteralPath '.env')) {
    throw 'Missing .env. Copy .env.example to .env and set private credentials first.'
}

docker compose up --build -d
if ($LASTEXITCODE -ne 0) {
    throw 'Docker Compose failed to start. Check Docker Desktop and the command output.'
}

Write-Host 'Airflow UI: http://localhost:8080'
Write-Host 'Inspect services with: docker compose ps'
