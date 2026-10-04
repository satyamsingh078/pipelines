# Kill the service in the middle of a burst and check nothing acknowledged is lost.
#   .\scripts\shutdown-test.ps1 -Signal SIGTERM     # graceful: expect "drained cleanly", exit code 0
#   .\scripts\shutdown-test.ps1 -Signal SIGKILL     # hard crash: no drain, container restarts
# Pass condition (in the final JSON report): "lost": 0 and "duplicates": 0.
# For Atlas add:  -ComposeFile docker-compose.atlas.yml
param(
  [ValidateSet("SIGTERM", "SIGKILL")] [string]$Signal = "SIGTERM",
  [int]$Events = 60000,
  [int]$Concurrency = 200,
  [int]$DelaySeconds = 4,
  [string]$ComposeFile = "docker-compose.yml"
)

docker compose -f $ComposeFile up -d --build
$dir = (Get-Location).Path

$job = Start-Job -ScriptBlock {
  param($dir, $file, $events, $conc)
  Set-Location $dir
  docker compose -f $file run --rm loadtest burst --events $events --concurrency $conc --dup-ratio 0.2 2>&1
} -ArgumentList $dir, $ComposeFile, $Events, $Concurrency

Start-Sleep -Seconds $DelaySeconds
Write-Host "`n>>> /stats just before the kill:"
curl.exe -s localhost:8080/stats
Write-Host "`n>>> sending $Signal"
docker compose -f $ComposeFile kill -s $Signal app
Start-Sleep -Seconds 3

Write-Host "`n>>> app logs:"
docker compose -f $ComposeFile logs app --tail 15
Write-Host "`n>>> container state:"
docker compose -f $ComposeFile ps -a

docker compose -f $ComposeFile up -d app
Write-Host "`n>>> waiting for the burst to finish; final report below"
Receive-Job -Job $job -Wait -AutoRemoveJob