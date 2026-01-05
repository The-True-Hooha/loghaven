$ErrorActionPreference = "Stop"

$REPO = "The-True-Hooha/loghaven"
$INSTALL_DIR = "$env:USERPROFILE\bin"

$LOGO = @"
██╗      ██████╗  ██████╗ ██╗  ██╗ █████╗ ██╗   ██╗███████╗███╗   ██╗
██║     ██╔═══██╗██╔════╝ ██║  ██║██╔══██╗██║   ██║██╔════╝████╗  ██║
██║     ██║   ██║██║  ███╗███████║███████║██║   ██║█████╗  ██╔██╗ ██║
██║     ██║   ██║██║   ██║██╔══██║██╔══██║╚██╗ ██╔╝██╔══╝  ██║╚██╗██║
███████╗╚██████╔╝╚██████╔╝██║  ██║██║  ██║ ╚████╔╝ ███████╗██║ ╚████║
╚══════╝ ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═══╝
"@

function Add-ToPath {
    param($Directory)
    
    $userPath = [Environment]::GetEnvironmentVariable("Path", "User")
    
    $alreadyInPath = $userPath -split ';' | Where-Object { $_ -eq $Directory }
    
    if (-not $alreadyInPath) {
        $newPath = if ($userPath) { "$userPath;$Directory" } else { $Directory }
        [Environment]::SetEnvironmentVariable("Path", $newPath, "User")
        Write-Host "✓ Added $Directory to PATH" -ForegroundColor Green
    } else {
        Write-Host "✓ $Directory already in PATH" -ForegroundColor Green
    }
    
    if ($env:Path -notlike "*$Directory*") {
        $env:Path = "$env:Path;$Directory"
        Write-Host "✓ Updated current session PATH" -ForegroundColor Green
    }
}

function Install-Local {
    Write-Host $LOGO -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Installing LogHaven from local build..." -ForegroundColor Yellow
    Write-Host ""
    
    $SOURCE = ".\target\release\loghaven.exe"
    
    if (-not (Test-Path $SOURCE)) {
        Write-Host "Building release binary..."
        cargo build --release
    }
    
    if (-not (Test-Path $INSTALL_DIR)) {
        New-Item -ItemType Directory -Path $INSTALL_DIR | Out-Null
        Write-Host "✓ Created $INSTALL_DIR" -ForegroundColor Green
    }
    
    $DEST = "$INSTALL_DIR\loghaven.exe"
    Copy-Item $SOURCE $DEST -Force
    
    Write-Host "✓ LogHaven installed to $DEST" -ForegroundColor Green
    
    Add-ToPath $INSTALL_DIR
    
    Write-Host ""
    Write-Host "Installation complete!" -ForegroundColor Green
}

function Install-Remote {
    param($Version)
    
    Write-Host $LOGO -ForegroundColor Cyan
    Write-Host ""
    
    if (-not $Version) {
        $response = Invoke-RestMethod -Uri "https://api.github.com/repos/$REPO/releases/latest"
        $Version = $response.tag_name
    }
    
    Write-Host "Installing LogHaven $Version for Windows..." -ForegroundColor Yellow
    Write-Host ""
    
    $BINARY_URL = "https://github.com/$REPO/releases/download/$Version/loghaven-windows-x86_64.exe"
    
    if (-not (Test-Path $INSTALL_DIR)) {
        New-Item -ItemType Directory -Path $INSTALL_DIR | Out-Null
        Write-Host "✓ Created $INSTALL_DIR" -ForegroundColor Green
    }
    
    $DEST = "$INSTALL_DIR\loghaven.exe"
    
    Invoke-WebRequest -Uri $BINARY_URL -OutFile $DEST
    
    Write-Host "✓ LogHaven installed to $DEST" -ForegroundColor Green
    
    Add-ToPath $INSTALL_DIR
    
    Write-Host ""
    Write-Host "Installation complete!" -ForegroundColor Green
}

function Main {
    param($Version)
    
    if (-not $Version) {
        try {
            Install-Remote
        }
        catch {
            Write-Host "No releases found, installing from local build..." -ForegroundColor Yellow
            Install-Local
        }
    }
    else {
        Install-Remote $Version
    }
    
    Write-Host ""
    Write-Host "Verify installation:" -ForegroundColor Cyan
    Write-Host "  loghaven --version"
    Write-Host "  loghaven init"
}

Main $args[0]