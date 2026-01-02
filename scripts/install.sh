#!bin/sh

set -e

REPO="The-True-Hooha/loghaven"
INSTALL_DIR="/usr/local/bin

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
        *)
            echo "Unsupported OS: $OS" >&2
            exit 1
            ;;
    esac
}

main() {
    PLATFORM=$(detect_platform)
    VERSION=${1:-$(get_latest_release)}
    
    echo "Installing LogHaven $VERSION for $PLATFORM..."
    
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
    
    echo "✓ LogHaven installed to $INSTALL_DIR/loghaven"
    echo ""
    echo "Verify installation:"
    echo "  loghaven --version"
}

main "$@"