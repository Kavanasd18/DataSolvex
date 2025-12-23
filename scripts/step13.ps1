
# Define the connection details
param
(
    [string]$serverName,
    [string]$databaseName, 
# Output directory
    [string]$outputDirectory 
)

# Query template with a placeholder for the object type
$queryTemplate = @"
SELECT 
    s.name+'.'+
    o.name AS [Name]
FROM 
    sys.sql_modules m
JOIN 
    sys.objects o ON m.object_id = o.object_id
JOIN 
    sys.schemas s ON o.schema_id = s.schema_id
WHERE 
    o.type = '@type'  -- This will be replaced by each parameter
    AND m.definition LIKE '%corpuser%'
ORDER BY 
    s.name, o.name;
"@


# List of parameters to pass into the query
$parameters = @("P", "V", "FN", "TR")

# Create SQL connection string for Windows Authentication and SSL
$connectionString = "Server=$serverName; Database=$databaseName; Integrated Security=True; Encrypt=True; TrustServerCertificate=True;"

# Function to execute the query with a given type and save the result
function Execute-Query {
    param (
        [string]$objectType, 
        [string]$outputFile
    )

    # Create SQL connection and command objects
    $connection = New-Object System.Data.SqlClient.SqlConnection
    $connection.ConnectionString = $connectionString
    $command = $connection.CreateCommand()
    
    # Replace @type placeholder in the query with the current object type
    $query = $queryTemplate.Replace("@type", $objectType)
    $command.CommandText = $query

    # Set Command Timeout (in seconds) to a higher value
    $command.CommandTimeout = 120  # 2 minutes timeout

    try {
        # Open the connection
        $connection.Open()

        # Execute the query and get the results
        $reader = $command.ExecuteReader()

        # Initialize an empty array to store the results
        $results = @()

        # Read the results
        while ($reader.Read()) {
            $results += $reader.GetString(0)
        }

        # Close the reader and connection
        $reader.Close()
        $connection.Close()

        # Save the results to the output file
        $results | Out-File -FilePath $outputFile -Encoding UTF8

        Write-Host "The results for '$objectType' have been saved to $outputFile"
    }
    catch {
        Write-Host "An error occurred while executing query for '$objectType': $_"
    }
    finally {
        # Ensure the connection is always closed
        if ($connection.State -eq 'Open') {
            $connection.Close()
        }
    }
}

# Loop over each parameter and execute the query
foreach ($param in $parameters) {
    # Determine the output file name based on the parameter
    switch ($param) {
        'P' { $outputFile = "$outputDirectory\SP.txt" }
        'V' { $outputFile = "$outputDirectory\View.txt" }
        'FN' { $outputFile = "$outputDirectory\Function.txt" }
        'TR' { $outputFile = "$outputDirectory\Trigger.txt" }
    }

    # Execute the query for the current parameter and save the output to the corresponding file
    Execute-Query -objectType $param -outputFile $outputFile
}

Write-Host "All queries executed and results saved."