use cex_core::{CPTKline, EXCHID_BINANCE, EXCHID_OKX, read_fixed};
use serde::{Deserialize, Serialize};


#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlineRecord {
    pub code: String,
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub open_time_ms: u64,
    pub local_ts_ms: u64,
    pub interval: i64,
    pub exchange_name: &'static str,
}


fn exchange_name(id: i32) -> &'static str {
    match id {
        EXCHID_BINANCE => "Binance",
        EXCHID_OKX => "OKX",
        _ => "Unknown",
    }
}

impl From<&CPTKline> for KlineRecord {
    fn from(value: &CPTKline) -> Self {
        Self {
            exchange_name: exchange_name(value.exchange),
            code: read_fixed(&value.code),
            open_time_ms: value.open_time_ms,
            interval: value.interval,
            local_ts_ms: value.local_ts_ms,
            open: value.open,
            close: value.close,
            high: value.high,
            low: value.low,
        }
    }
}