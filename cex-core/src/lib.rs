use thiserror::Error;

pub mod shm_chan;
mod v1;

pub use v1::*;
pub use shm_chan::*;

pub const CHANNEL_CAP: usize = 1024;
pub const CH_KLINE_V1: &str = "CTPKlineV1";
pub const CH_TRADE_V1: &str = "CTPTradeV1";
pub const CH_ORDER_V1: &str = "CTPOrderV1";
pub const CH_SNAPSHOT_V1: &str = "CTPSnapshotV1";

pub const ORDER_STATUS_NEW: i32 = 1;
pub const ORDER_STATUS_PARTIAL: i32 = 2;
pub const ORDER_STATUS_FILLED: i32 = 3;
pub const ORDER_STATUS_CANCELLED: i32 = 4;

pub const SIDE_OPEN_LONG: i32 = 10;
pub const SIDE_CLOSE_LONG: i32 = 11;
pub const SIDE_OPEN_SHORT: i32 = 20;
pub const SIDE_CLOSE_SHORT: i32 = 21;


pub const ORDER_TYPE_LIMIT: i32 = 300;
pub const ORDER_TYPE_MARKET: i32 = 301;

pub const EXCHID_BINANCE: i32 = 4000;
pub const EXCHID_OKX: i32 = 4001;

#[derive(Debug, Error)]
pub enum CexError {
    #[error("API error: {0}")]
    ApiError(String),
    #[error("Network error: {0}")]
    NetworkError(String),
    #[error("Parse error: {0}")]
    ParseError(String),
}
