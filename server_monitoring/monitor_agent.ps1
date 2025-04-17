param (
    [string]$ServerName
)

$ServiceName = "HealthService"

try {
    $service = Get-Service -ComputerName $ServerName -Name $ServiceName -ErrorAction Stop

    if ($service.Status -eq "Running") {
        $action = "Already Running"
    } else {
        Restart-Service -InputObject $service -Force -ErrorAction Stop
        Start-Sleep -Seconds 5
        $service = Get-Service -ComputerName $ServerName -Name $ServiceName

        $action = if ($service.Status -eq "Running") {
            "Restarted"
        } else {
            "Failed to Start"
        }
    }

    @{
        Server      = $ServerName
        Status      = $service.Status
        Action      = $action
        ServiceName = $ServiceName
    } | ConvertTo-Json -Compress
}
catch {
    $message = $_.Exception.Message

    if ($message -like "*RPC server is unavailable*") {
        $message = "Unreachable: RPC Server Unavailable"
    } elseif ($message -like "*Access is denied*") {
        $message = "Unreachable: Access Denied"
    } elseif ($message -like "*Cannot find any service with service name*") {
        $message = "Unreachable: Service Not Found"
    } elseif ($message -like "*The network path was not found*") {
        $message = "Unreachable: Network Path Not Found"
    }

    @{
        Server      = $ServerName
        Status      = "Error"
        Action      = $message
        ServiceName = $ServiceName
    } | ConvertTo-Json -Compress
}
