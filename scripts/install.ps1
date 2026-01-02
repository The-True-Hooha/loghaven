$ErrorActionPreference = "Stop"

$REPO = "The-True-Hooha/loghaven"
$INSTALL_DIR = "$env:USERPROFILE\bin"

function Get-LatestRelease {
    $response = Invoke-RestMethod -Uri "https://api.github.com/repos/$REPO/releases/latest"
    return $response.tag_name
}

function Main {
    param($Version)
    
    if (-not $Version) {
        $Version = Get-LatestRelease
    }
    
    Write-Host "Installing LogHaven $Version for Windows..."
    
    $BINARY_URL = "https://github.com/$REPO/releases/download/$Version/loghaven-windows-x86_64.exe"
    
    if (-not (Test-Path $INSTALL_DIR)) {
        New-Item -ItemType Directory -Path $INSTALL_DIR | Out-Null
    }
    
    $DEST = "$INSTALL_DIR\loghaven.exe"
    
    Invoke-WebRequest -Uri $BINARY_URL -OutFile $DEST
    
    $envPath = [Environment]::GetEnvironmentVariable("Path", "User")
    if ($envPath -notlike "*$INSTALL_DIR*") {
        [Environment]::SetEnvironmentVariable("Path", "$envPath;$INSTALL_DIR", "User")
        Write-Host "Added $INSTALL_DIR to PATH (restart shell to use)"
    }
    
    Write-Host "✓ LogHaven installed to $DEST"
    Write-Host ""
    Write-Host "Verify installation (in new shell):"
    Write-Host "  loghaven --version"
}

Main $args[0]