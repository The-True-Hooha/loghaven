//! Chain adapters turn on-chain events into log records. They run in their own
//! process (`loghaven watch <chain>`) and send through the normal signed ingest path.

pub mod ethereum;
pub mod rpc;
pub mod solana;
pub mod stellar;
pub mod watch;
pub mod ws;
