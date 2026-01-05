#!/bin/sh
set -e

REPO="The-True-Hooha/loghaven"
INSTALL_DIR="/usr/local/bin"

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

print_logo() {
    echo "${CYAN}"
    cat << "EOF"
██╗      ██████╗  ██████╗ ██╗  ██╗ █████╗ ██╗   ██╗███████╗███╗   ██╗
██║     ██╔═══██╗██╔════╝ ██║  ██║██╔══██╗██║   ██║██╔════╝████╗  ██║
██║     ██║   ██║██║  ███╗███████║███████║██║   ██║█████╗  ██╔██╗ ██║
██║     ██║   ██║██║   ██║██╔══██║██╔══██║╚██╗ ██╔╝██╔══╝  ██║╚██╗██║
███████╗╚██████╔╝╚██████╔╝██║  ██║██║  ██║ ╚████╔╝ ███████╗██║ ╚████║
╚══════╝ ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═══╝
EOF
    echo "${NC}"
    echo ""
}

get_latest_release() {
    curl --silent "https://api.github.com/repos/$REPO/releases/latest" |
        grep '"tag_name":' |
        sed -E 's/.*"([^"]+)".*/\1/'
}

detect_platform() {
    OS="$(uname -s)"
    ARCH="$(uname -m)"
    
    case "$OS" in
        Linux*)
            case "$ARCH" in
                x86_64) echo "linux-x86_64" ;;
                *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
            esac
            ;;
        Darwin*)
            case "$ARCH" in
                x86_64) echo "macos-x86_64" ;;
                arm64) echo "macos-arm64" ;;
                *) echo "Unsupported architecture: $ARCH" >&2; exit 1 ;;
            esac
            ;;
        MINGW*|MSYS*|CYGWIN*)
            echo "${YELLOW}Use PowerShell install script on Windows:${NC}" >&2
            echo "  irm https://raw.githubusercontent.com/$REPO/main/scripts/install.ps1 | iex" >&2
            exit 1
            ;;
        *)
            echo "Unsupported OS: $OS" >&2
            exit 1
            ;;
    esac
}

main() {
    print_logo
    
    PLATFORM=$(detect_platform)
    VERSION=${1:-$(get_latest_release)}
    
    echo "${YELLOW}Installing LogHaven $VERSION for $PLATFORM...${NC}"
    echo ""
    
    BINARY_URL="https://github.com/$REPO/releases/download/$VERSION/loghaven-$PLATFORM"
    TEMP_FILE=$(mktemp)
    
    curl -L -o "$TEMP_FILE" "$BINARY_URL"
    chmod +x "$TEMP_FILE"
    
    if [ -w "$INSTALL_DIR" ]; then
        mv "$TEMP_FILE" "$INSTALL_DIR/loghaven"
    else
        echo "Installing to $INSTALL_DIR requires sudo..."
        sudo mv "$TEMP_FILE" "$INSTALL_DIR/loghaven"
    fi
    
    echo "${GREEN}✓ LogHaven installed to $INSTALL_DIR/loghaven${NC}"
    echo ""
    echo "${GREEN}Installation complete!${NC}"
    echo ""
    echo "${CYAN}Verify installation:${NC}"
    echo "  loghaven --version"
    echo "  loghaven init"
}

main "$@"