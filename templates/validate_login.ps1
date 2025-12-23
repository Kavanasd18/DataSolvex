param (
    [string]$Username  ,
    [string]$Password
)

# Import Active Directory Module
Import-Module ActiveDirectory

# Convert the password to a SecureString
$SecurePasswordString = ConvertTo-SecureString $Password -AsPlainText -Force

# Create the PSCredential object for authentication
$ADCredential = New-Object System.Management.Automation.PSCredential($Username, $SecurePasswordString)

# Try to authenticate the user by getting the AD user
try {
    # Attempt to get the user from Active Directory to validate the credentials
    $User = Get-ADUser -Identity $Username -Credential $ADCredential -ErrorAction Stop

    # If successful, check the user's group membership
    $UserGroups = Get-ADUser -Identity $Username -Properties MemberOf

    # Check if the user is a member of the "EPMDBADMIN" group
    $GroupName = "EPMDBADMIN"
    $IsInGroup = $UserGroups.MemberOf | Where-Object { $_ -like "*$GroupName*" }

    if ($IsInGroup) {
        Write-Host "Login Successful! Welcome, $Username."
    }
    else {
        Write-Host "Login Failed! You are not a member of the '$GroupName' group."
    }
}
catch {
    # If the credentials are incorrect or there was an error, display login failed message
    Write-Host "Login Failed! Please check your username and password."
}
