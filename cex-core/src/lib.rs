pub mod shm_chan;
mod v1;

pub use v1::*;
pub use shm_chan::*;

/// Channel capacity: 1 day of per-second data
pub const CHANNEL_CAP: usize = 100 * 3600 * 24;
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

/// 现货
pub const COIN_TYPE_SPOT: i32 = 300;
/// 杠杆(两融)
pub const COIN_TYPE_MARGIN: i32 = 301;
/// 永续合约
pub const COIN_TYPE_SWAP: i32 = 400;
/// 交割合约
pub const COIN_TYPE_FUTURES: i32 = 401;
/// 期权
pub const COIN_TYPE_OPTION: i32 = 402;

/// 限价委托
pub const ORDER_TYPE_LIMIT: i32 = 1000;
/// 市价委托
pub const ORDER_TYPE_MARKET: i32 = 1001;
/// 只挂单
pub const ORDER_TYPE_POST_ONLY: i32 = 1002;
/// 全部成交或立即取消
pub const ORDER_TYPE_FOK: i32 = 1003;
/// 立即成交并取消剩余
pub const ORDER_TYPE_IOC: i32 = 1004;
/// 市价委托立即成交并取消剩余
pub const ORDER_TYPE_OPTIMAL_LIMIT_IOC: i32 = 1005;


/// Binance
pub const EXCHID_BINANCE: i32 = 4000;
/// OKX
pub const EXCHID_OKX: i32 = 4001;
