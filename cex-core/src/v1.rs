use std::fmt::Display;

use chrono::TimeZone;
use zerocopy::{FromBytes, IntoBytes};


// --- FFI-compatible definitions ---
// Use fixed-size buffers for strings so the layout is predictable for C.
#[repr(C, packed(1))]
#[derive(Copy, Clone, Debug, IntoBytes, FromBytes)]
pub struct CPTKline {
    pub code: [u8; 16],
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub exchange: i32,
    pub data_type: i32,
    pub open_time_ms: u64,
    pub interval: i64,
    pub local_ts_ms: u64,
}


#[repr(C, packed(1))]
#[derive(Copy, Clone, Debug, IntoBytes, FromBytes)]
pub struct CPTSnapshot {
    pub code: [u8; 16],
    pub open: f64,
    pub close: f64,
    pub high: f64,
    pub low: f64,
    pub ask_vol: [u64; 10],
    pub ask_prx: [f64; 10],
    pub bid_vol: [u64; 10],
    pub bid_prx: [f64; 10],
    pub exchange: i32,
    pub data_type: i32,
    pub open_time_ms: u64,
    pub interval: i64,
}

// Minimal POD order for matching and IPC
#[repr(C, packed(1))]
#[derive(Copy, Clone, Debug, IntoBytes, FromBytes)]
pub struct CPTOrder {
    pub id: u64,
    pub client_id: u64,
    pub code: [u8; 16],
    pub side: i32,
    pub order_type: i32,
    pub price: f64,
    pub quantity: u64,
    pub remaining: u64,
    pub filled: u64,
    pub status: i32,
    pub exchange: i32,
    pub data_type: i32,
    pub timestamp_ns: u64,
    pub localts: u64,
}

#[repr(C, packed(1))]
#[derive(Copy, Clone, Debug, IntoBytes, FromBytes)]
pub struct CPTTransaction {
    pub id: u64,
    pub ask_order_id: u64,
    pub bid_order_id: u64,
    pub code: [u8; 16],
    pub price: f64,
    pub quantity: u64,
    pub trade_type: i32, // 1: trade, 2: cancel
    pub exchange: i32,
    pub data_type: i32,
    pub timestamp_ns: u64,
    pub localts: u64,
}


pub fn write_fixed(dst: &mut [u8], s: &str) {
    let bytes = s.as_bytes();
    let len = bytes.len().min(dst.len());
    dst[..len].copy_from_slice(&bytes[..len]);
    if len < dst.len() {
        for b in &mut dst[len..] {
            *b = 0;
        }
    }
}

pub fn read_fixed(src: &[u8]) -> String {
    let len = src.iter().position(|&b| b == 0).unwrap_or(src.len());
    String::from_utf8_lossy(&src[..len]).to_string()
}

impl Display for CPTKline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let exchange = match self.exchange {
            super::EXCHID_BINANCE => "Binance",
            super::EXCHID_OKX => "OKX",
            _ => "Unknown",
        };
        let data_type = match self.data_type {
            super::COIN_TYPE_SPOT => "Spot",
            super::COIN_TYPE_MARGIN => "Margin",
            super::COIN_TYPE_SWAP => "Swap",
            super::COIN_TYPE_FUTURES => "Futures",
            super::COIN_TYPE_OPTION => "Option",
            _ => "Unknown",
        };
        let symbol = self.code;
        let open_time_ms = self.open_time_ms;
        let utc_ts =  chrono::Utc.timestamp_millis_opt(open_time_ms as i64).unwrap();
        let local_time = utc_ts.with_timezone(&chrono::Local);
        let open_time_human_readable = local_time.format("%Y-%m-%d %H:%M:%S").to_string();

        let interval = self.interval;
        let local_ts_ns = self.local_ts_ms;
        let open = self.open;
        let close = self.close;
        let high = self.high;
        let low = self.low;

        write!(
            f,
            "Kline{{ code: {}, open: {}, close: {}, high: {}, low: {}, exch: {}, type: {}, open_ts: {}, open_time_ms: {}, local_ts_ns: {}, interval: {}}}",
            read_fixed(&symbol),
            open,
            close,
            high,
            low,
            exchange,
            data_type,
            open_time_human_readable,
            open_time_ms,
            local_ts_ns,
            interval,
        )
    }
}