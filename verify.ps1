$ErrorActionPreference = 'Stop'
Set-Location $PSScriptRoot

function Invoke-Compose {
    param([Parameter(ValueFromRemainingArguments)] [string[]]$Arguments)
    docker compose @Arguments
    if ($LASTEXITCODE -ne 0) { throw "docker compose $Arguments failed ($LASTEXITCODE)" }
}

Invoke-Compose config --quiet
Invoke-Compose up -d --build --wait
Invoke-Compose run --rm --build demo
Invoke-Compose stop stream
try {
    Invoke-Compose exec -T ingest python app.py seed-crash
    docker compose run --rm --no-deps stream python app.py stream --once --crash-before-commit
    if ($LASTEXITCODE -ne 77) { throw "Expected simulated crash exit 77, got $LASTEXITCODE" }
    $state = Invoke-Compose exec -T ingest python app.py snapshot | ConvertFrom-Json
    if ($state.pending -ne 1 -or $state.events -ne 7 -or $state.raw_events -ne 21) {
        throw 'Unexpected state after rollback'
    }
    Write-Output 'PASS rollback: input remains pending, canonical insert rolled back'
} finally {
    Invoke-Compose start stream
}
Invoke-Compose exec -T ingest python app.py check-recovery
$port = if ($env:VK_P2_PORT) { $env:VK_P2_PORT } else { '18082' }
Invoke-RestMethod "http://127.0.0.1:$port/metrics" | ConvertTo-Json -Depth 8
