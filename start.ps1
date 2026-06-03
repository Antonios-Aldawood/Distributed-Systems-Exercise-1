# start.ps1 — launches all five service windows for the distributed systems demo.
#
# Service layout:
#   :8080  gateway          — load balancer, circuit breaker, retry, timeout
#   :8081  user-service     — Users CRUD, SQLite
#   :8082  product-service A — Products CRUD, SQLite (fast instance)
#   :8083  product-service B — Products CRUD, SQLite (slow instance, LATENCY_MS=400)
#   :8084  order-service    — Orders+Items CRUD, SQLite, idempotency
#
# product-service B has artificial latency so you can observe the gateway
# preferring the faster instance and the circuit breaker tripping when you
# set its LATENCY_MS high enough to exceed the 5-second timeout.
#
# To switch to Least-Connections load balancing, set LB_ALGO=least-connections
# on the gateway window after starting.

$root = $PSScriptRoot

function Start-Svc {
    param(
        [string]$Title,
        [string]$Dir,
        [hashtable]$Env
    )
    $envLines = ($Env.GetEnumerator() | ForEach-Object {
        "`$env:$($_.Key) = '$($_.Value)'"
    }) -join "; "

    $cmd = "`$host.UI.RawUI.WindowTitle = '$Title'; $envLines; Set-Location '$Dir'; go run .; Read-Host 'Press Enter to close'"
    Start-Process powershell -ArgumentList "-NoExit", "-Command", $cmd
    Start-Sleep -Milliseconds 300
}

Write-Host ""
Write-Host "Starting distributed systems demo..." -ForegroundColor Cyan
Write-Host ""

Start-Svc -Title "user-service  :8081" -Dir "$root\user-service" -Env @{
    PORT = "8081"
}

Start-Svc -Title "product-svc A :8082" -Dir "$root\product-service" -Env @{
    PORT = "8082"
}

Start-Svc -Title "product-svc B :8083 (slow)" -Dir "$root\product-service" -Env @{
    PORT       = "8083"
    # $env:LATENCY_MS = '5500'
    LATENCY_MS = "5500"   # simulated latency — raise to 5500 to trip the circuit breaker
}

Start-Svc -Title "order-service :8084" -Dir "$root\order-service" -Env @{
    PORT               = "8084"
    PRODUCT_SERVICE_URL = "http://localhost:8082"
}

Start-Svc -Title "gateway       :8080" -Dir "$root\gateway" -Env @{
    PORT    = "8080"
    # LB_ALGO = "least-connections"   # uncomment to switch algorithm
}

Write-Host ""
Write-Host "All services starting. Allow a few seconds for compilation." -ForegroundColor Green
Write-Host ""
Write-Host "── Quick-start commands ─────────────────────────────────────────" -ForegroundColor Yellow
Write-Host ""
Write-Host "# 1. Create a user"
Write-Host "Invoke-RestMethod -Method POST -Uri http://localhost:8080/users ``"
Write-Host "  -ContentType 'application/json' ``"
Write-Host "  -Body '{""name"":""Alice"",""email"":""alice@example.com""}'"
Write-Host ""
Write-Host "# 2. Create a product"
Write-Host "Invoke-RestMethod -Method POST -Uri http://localhost:8080/products ``"
Write-Host "  -ContentType 'application/json' ``"
Write-Host "  -Body '{""name"":""Widget"",""price"":9.99}'"
Write-Host ""
Write-Host "# 3. Create an order (with idempotency key — safe to repeat)"
Write-Host "Invoke-RestMethod -Method POST -Uri http://localhost:8080/orders ``"
Write-Host "  -ContentType 'application/json' ``"
Write-Host "  -Headers @{'Idempotency-Key'='demo-order-001'} ``"
Write-Host "  -Body '{""user_id"":1,""items"":[{""product_id"":1,""quantity"":2}]}'"
Write-Host ""
Write-Host "# 4. Watch the load balancer and circuit breakers live"
Write-Host "Invoke-RestMethod http://localhost:8080/gateway/status | ConvertTo-Json -Depth 5"
Write-Host ""
Write-Host "# 5. Simulate a slow backend — edit LATENCY_MS on product-svc B window"
Write-Host "#    Set it to 5500 ms and watch the circuit breaker open on :8083"
Write-Host ""
Write-Host "────────────────────────────────────────────────────────────────" -ForegroundColor Yellow
