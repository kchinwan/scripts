param (
    [string]$ServerName
)

$ServiceName = "HealthService"

try {
    $service = Get-Service -Name $ServiceName -ComputerName $ServerName -ErrorAction Stop

    if ($service.Status -ne 'Running') {
        Start-Service -InputObject $service
        Start-Sleep -Seconds 5  # Wait briefly to confirm it starts

        # Re-check the service status
        $service = Get-Service -Name $ServiceName -ComputerName $ServerName -ErrorAction Stop

        $action = if ($service.Status -eq 'Running') {
            "Service was restarted successfully"
        } else {
            "Tried to restart but service is still not running"
        }
    } else {
        $action = "Service is already running"
    }

    @{
        Server      = $ServerName
        Status      = $service.Status
        Action      = $action
        ServiceName = $ServiceName
    } | ConvertTo-Json -Compress

} catch {
    $errorMsg = $_.Exception.Message
    @{
        Server      = $ServerName
        Status      = "Error"
        Action      = "Unreachable or error: $errorMsg"
        ServiceName = $ServiceName
    } | ConvertTo-Json -Compress
}
