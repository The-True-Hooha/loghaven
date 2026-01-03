# LogHaven

```cmd
██╗      ██████╗  ██████╗ ██╗  ██╗ █████╗ ██╗   ██╗███████╗███╗   ██╗
██║     ██╔═══██╗██╔════╝ ██║  ██║██╔══██╗██║   ██║██╔════╝████╗  ██║
██║     ██║   ██║██║  ███╗███████║███████║██║   ██║█████╗  ██╔██╗ ██║
██║     ██║   ██║██║   ██║██╔══██║██╔══██║╚██╗ ██╔╝██╔══╝  ██║╚██╗██║
███████╗╚██████╔╝╚██████╔╝██║  ██║██║  ██║ ╚████╔╝ ███████╗██║ ╚████║
╚══════╝ ╚═════╝  ╚═════╝ ╚═╝  ╚═╝╚═╝  ╚═╝  ╚═══╝  ╚══════╝╚═╝  ╚═══╝
```

A local-first, composable, chain-aware observability runtime for on-chain and off-chain systems

## Overview

LogHaven is a **local-first, chain-agnostic observability runtime** that ingests telemetry data locally with complete storage control. Unlike traditional SaaS observability platforms that charge per ingestion and lock you into proprietary systems, LogHaven gives you full control over your data while providing powerful cross-chain correlation capabilities.

### Key Features

- Local-First Architecture - Your data stays on your infrastructure by default
- Chain-Agnostic - Unified observability across Ethereum, Solana, Stellar, and more
- Bring Your Own Storage - Support for local filesystem, S3, R2, MinIO
- Transaction-Centric - Correlate on-chain transactions with off-chain logs
- Privacy-First - Client-side encryption, no vendor lock-in
- Zero Ingestion Fees - No usage-based pricing traps

## Quick Start

### Installation

**Linux / macOS:**

```bash
curl -fsSL https://raw.githubusercontent.com/The-True-Hooha/loghaven/main/scripts/install.sh | sh
```

**Windows (PowerShell):**

```powershell
irm https://raw.githubusercontent.com/The-True-Hooha/loghaven/main/scripts/install.ps1 | iex
```

**From Source:**

```bash
cargo install --git https://github.com/The-True-Hooha/loghaven
```

### Verify Installation

```bash
loghaven --version
```

### Initialize Configuration

```bash
loghaven init
```

This creates a default configuration file at:

- **Linux/macOS:** `~/.config/loghaven/config.toml`
- **Windows:** `%APPDATA%\loghaven\config.toml`

## Architecture

### Design Philosophy

LogHaven follows a **separation of concerns** architecture built on these core principles:

#### 1. Local-First by Default

- Agent runs on your infrastructure
- No external services required for core functionality
- Network calls are explicit and opt-in

#### 2. Daemon-Based Runtime

- Long-running background process handles data ingestion
- CLI communicates with daemon via IPC (Unix sockets/named pipes)
- Daemon owns all stateful operations (storage, processing, indexing)

#### 3. Pluggable Storage Backends

- Abstract storage interface
- Multiple simultaneous backends (write local, sync to cloud)
- Consistent data format across all backends

#### 4. Chain Adapters as Plugins

- Each blockchain is an independent module
- Normalized event format across chains
- Adding new chains doesn't require core changes

### System Components

```cmd
┌──────────────────────────────────────────────────────────┐
│                     User Interface                        │
│  ┌─────────┐  ┌──────────┐  ┌─────────┐  ┌──────────┐   │
│  │   CLI   │  │    TUI   │  │Dashboard│  │   API    │   │
│  └────┬────┘  └─────┬────┘  └────┬────┘  └─────┬────┘   │
└───────┼────────────┼─────────────┼─────────────┼─────────┘
        │            │             │             │
        └────────────┴─────────────┴─────────────┘
                            │
                            ▼
                ┌───────────────────────┐
                │    IPC Layer          │
                │  (Unix Socket / Pipe) │
                └───────────┬───────────┘
                            │
                            ▼
        ┌───────────────────────────────────────┐
        │        Daemon Runtime (Core)          │
        │  ┌─────────────┐  ┌────────────────┐ │
        │  │   Ingestion │  │  Transaction   │ │
        │  │   Pipeline  │  │    Indexer     │ │
        │  └──────┬──────┘  └────────┬───────┘ │
        │         │                  │          │
        │  ┌──────▼──────────────────▼───────┐ │
        │  │    Event Processing Engine      │ │
        │  └──────┬──────────────────┬───────┘ │
        └─────────┼──────────────────┼─────────┘
                  │                  │
        ┌─────────▼─────┐  ┌─────────▼──────┐
        │ Chain Adapters │  │ Storage Engine │
        │  - Ethereum    │  │  - Local FS    │
        │  - Solana      │  │  - S3/R2       │
        │  - Stellar     │  │  - MinIO       │
        └────────────────┘  └────────────────┘
```

### Why This Architecture?

#### Daemon-Based Design

- Persistent State: Long-running process maintains connections to RPCs and storage
- Resource Efficiency: Single process handles all chains, no per-chain overhead
- Graceful Lifecycle: Clean startup/shutdown with crash recovery
- IPC Simplicity: CLI is lightweight, daemon does the heavy lifting

#### Storage Abstraction

- Vendor Independence: Switch storage backends without data migration
- Hybrid Deployments: Write locally for speed, backup to cloud for durability
- Cost Control: Use cheap local storage for active data, archive to object storage

#### Plugin-Based Chains

- Extensibility: Community can add chains without modifying core
- Maintenance: Chain-specific bugs don't affect other chains
- Testing: Mock chain adapters for integration tests

## Configuration Guide

### Accessing Your Configuration

**Linux/macOS:**

```bash
# View configuration
cat ~/.config/loghaven/config.toml

# Edit with default editor
${EDITOR:-nano} ~/.config/loghaven/config.toml

# Edit with specific editor
vim ~/.config/loghaven/config.toml
code ~/.config/loghaven/config.toml
```

**Windows (PowerShell):**

```powershell
# View configuration
Get-Content $env:APPDATA\loghaven\config.toml

# Edit with Notepad
notepad $env:APPDATA\loghaven\config.toml

# Edit with VS Code
code $env:APPDATA\loghaven\config.toml
```

### Configuration File Structure

```toml
[meta]
version = 1

[agent]
name = "loghaven-agent"
log_level = "info"  # trace | debug | info | warn | error

[daemon]
socket_path = "~/.loghaven/loghaven.sock"  # Unix socket (Linux/macOS)
tcp_port = 9090  # TCP port (Windows or fallback)

[storage]
backend = "local"  # local | s3 | r2 | minio
primary = "local"
# secondary = "s3"  # Optional: backup to cloud

[storage.local]
path = "~/.loghaven/data"
max_size_gb = 10

# [storage.s3]
# bucket = "my-logs"
# region = "us-east-1"
# prefix = "loghaven/"

# [storage.r2]
# account_id = "your-account-id"
# bucket = "my-logs"
# prefix = "loghaven/"

# [storage.minio]
# endpoint = "http://localhost:9000"
# bucket = "loghaven"
# access_key = "minioadmin"
# secret_key = "minioadmin"

[chains]
enabled = []  # Add chains you want to monitor

# [chains.ethereum]
# rpc_url = "https://eth-mainnet.g.alchemy.com/v2/YOUR-KEY"
# ws_url = "wss://eth-mainnet.g.alchemy.com/v2/YOUR-KEY"

# [chains.solana]
# rpc_url = "https://api.mainnet-beta.solana.com"
# ws_url = "wss://api.mainnet-beta.solana.com"

# [chains.stellar]
# horizon_url = "https://horizon.stellar.org"
```

### Configuration Profiles

Create different configurations for different environments:

```bash
# Create dev profile
loghaven init --profile dev

# Create prod profile with S3 storage
loghaven init --profile prod --storage s3

# Run with specific profile
loghaven run --profile dev
loghaven run --profile prod
```

Profile locations:

- **Linux/macOS:** `~/.config/loghaven/{profile}.toml`
- **Windows:** `%APPDATA%\loghaven\{profile}.toml`

### Environment Variable Overrides

Override configuration without editing files:

**Linux/macOS:**

```bash
export LOGHAVEN_STORAGE_BACKEND=s3
export LOGHAVEN_LOG_LEVEL=debug
export LOGHAVEN_DAEMON_PORT=9091
loghaven run
```

**Windows (PowerShell):**

```powershell
$env:LOGHAVEN_STORAGE_BACKEND="s3"
$env:LOGHAVEN_LOG_LEVEL="debug"
$env:LOGHAVEN_DAEMON_PORT=9091
loghaven run
```

**Windows (Command Prompt):**

```cmd
set LOGHAVEN_STORAGE_BACKEND=s3
set LOGHAVEN_LOG_LEVEL=debug
set LOGHAVEN_DAEMON_PORT=9091
loghaven run
```

### Common Configuration Tasks

**Change Storage Backend:**

```bash
loghaven init --storage s3 --force
```

**Enable Ethereum Monitoring:**

Edit your config file and add:

```toml
[chains]
enabled = ["ethereum"]

[chains.ethereum]
rpc_url = "https://eth-mainnet.g.alchemy.com/v2/YOUR-KEY"
```

**Increase Storage Quota:**

```toml
[storage.local]
max_size_gb = 50  # Increase from 10GB to 50GB
```

**Change Log Level:**

```toml
[agent]
log_level = "debug"  # Change from "info" to "debug"
```

## Development

### Build from Source

```bash
git clone https://github.com/The-True-Hooha/loghaven.git
cd loghaven
cargo build --release
```

### Run Tests

```bash
cargo test
```

### Local Development

```bash
# Make changes
cargo build

# Test locally
cargo run -- init
cargo run -- run

# Release build
cargo build --release
./target/release/loghaven --version
```

## Project Structure

```cmd
loghaven/
├── src/
│   ├── cli/              # Command-line interface
│   │   ├── mod.rs        # CLI structure and routing
│   │   ├── commands.rs   # Command implementations
│   │   └── style.rs      # Terminal styling and output
│   ├── config/           # Configuration management
│   │   ├── mod.rs        # Config structures and loading
│   │   ├── defaults.rs   # Default values
│   │   └── validation.rs # Config validation
│   ├── error.rs          # Error types
│   └── main.rs           # Entry point
├── scripts/
│   ├── install.sh        # Unix installation script
│   └── install.ps1       # Windows installation script
├── .github/
│   └── workflows/
│       ├── ci.yml        # Pull request checks
│       ├── release.yml   # Stable releases
│       └── nightly.yml   # Weekly dev builds
└── Cargo.toml
```

## Philosophy

**We believe observability should be:**

- **User-Owned** - Your data belongs to you, not a SaaS vendor
- **Cost-Predictable** - No surprise bills from ingestion spikes
- **Privacy-Respecting** - Sensitive logs stay on your infrastructure
- **Chain-Agnostic** - Web3 development spans multiple blockchains
- **Composable** - Build custom workflows, not rigid dashboards

**LogHaven is NOT:**

- A managed SaaS platform (we don't host your data)
- A replacement for APM tools (we complement them)
- Blockchain-specific (we support any chain via adapters)
- A closed ecosystem (bring your own storage, tools, workflows)

## Contributing

LogHaven is in active development. Contributions are welcome!

## License

Coming Soon!

## Links

- **Website:** [https://loghaven.dev](https://loghaven.dev)
- **Documentation:** [https://loghaven.dev/docs](https://loghaven.dev/docs)
- **Issues:** <https://github.com/The-True-Hooha/loghaven/issues>
- **Discussions:** <https://github.com/The-True-Hooha/loghaven/discussions>
