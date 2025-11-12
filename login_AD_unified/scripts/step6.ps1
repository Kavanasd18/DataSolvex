param (
    [string]$rootFolderPath,
    [string]$Fromsch ,
    [string]$Tosch ,
    [string]$FromDb = "corpdata",
    [string]$ToDb = "testdb" 
)

# Get all subdirectories from the root folder
$folderPaths = Get-ChildItem -Path $rootFolderPath -Directory

foreach ($folderPath in $folderPaths) {
    $sqlFiles = Get-ChildItem -Path $folderPath.FullName -Filter *.sql

    foreach ($file in $sqlFiles) {
        $content = Get-Content -Path $file.FullName

        # Content replacement using dynamic values and case-insensitive regex
        $updatedContent = $content `
            -replace "(?i)\b$Fromsch\b", $Tosch `
            -replace '(?i)\bcreate\s+proc', 'alter proc' `
            -replace '(?i)\bcreate\s+view', 'alter view' `
            -replace '(?i)\bcreate\s+function', 'alter function' `
            -replace '(?i)\bcreate\s+trigger', 'alter trigger' `
            -replace '(?i)\bcreate\s+type', 'alter type' `
            -replace '(?i)SET QUOTED_IDENTIFIER ON', 'GO' `
            -replace '(?i)SET QUOTED_IDENTIFIER OFF', 'GO' `
            -replace "(?i)\b$FromDb\b", $ToDb

        # Save the updated content back to the file
        Set-Content -Path $file.FullName -Value $updatedContent
    }
}

Write-Host "Got successful"