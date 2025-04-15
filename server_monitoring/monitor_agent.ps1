param (
    [string]$ServerName
)

try {
    $service = Get-Service -ComputerName $ServerName -Name "HealthService" -ErrorAction Stop

    if ($service.Status -ne "Running") {
        Restart-Service -InputObject $service -Force -ErrorAction Stop
        Start-Sleep -Seconds 5

        # Refresh service status
        $service = Get-Service -ComputerName $ServerName -Name "HealthService"
    }

    $action = if ($service.Status -eq "Running") {
        if ($service.Status -ne "Running") { "Restarted" } else { "Already Running" }
    } else {
        "Failed to Start"
    }

    @{
        Server = $ServerName
        Status = $service.Status
        Action = $action
    } | ConvertTo-Json -Compress
}
catch {
    @{
        Server = $ServerName
        Status = "Error"
        Action = $_.Exception.Message
    } | ConvertTo-Json -Compress
}
