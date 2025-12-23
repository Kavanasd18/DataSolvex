# Path to Python executable
$pythonPath = "C:\Program Files\Python312\python.exe"  # Adjust with your Python installation path

# Path to the Python script to run
$scriptPath = "E:\Login_auth\app.py"  # Adjust with the path to your script

# Loop to keep the script running indefinitely
while ($true) {
    # Start the Python script
    $process = Start-Process -FilePath $pythonPath -ArgumentList $scriptPath -PassThru

    # Wait for the process to exit
    $process.WaitForExit()

    # Log exit (you can customize this part to suit your needs, like logging to a file)
    Write-Host "Python script exited with code $($process.ExitCode). Restarting..." -ForegroundColor Red

    # Optional: Add a delay before restarting (if desired)
    Start-Sleep -Seconds 5
}
