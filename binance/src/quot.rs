use anyhow::bail;
use log::{error, info};
use serde::{Deserialize, Serialize};
use cex_core::{CPTKline, EXCHID_BINANCE, Sender, write_fixed};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message};

/*
{
    "stream": "btcusdt@kline_1m",
    "data": {
        "e": "kline",
        "E": 1748877604023,
        "s": "BTCUSDT",
        "k": {
            "t": 1748877600000,
            "T": 1748877659999,
            "s": "BTCUSDT",
            "i": "1m",
            "f": 4978109970,
            "L": 4978110557,
            "o": "104349.06000000",
            "c": "104380.96000000",
            "h": "104380.96000000",
            "l": "104349.06000000",
            "v": "10.32405000",
            "n": 588,
            "x": false,
            "q": "1077392.54360710",
            "V": "10.27943000",
            "Q": "1072735.25781810",
            "B": "0"
        }
    }
}
*/
#[derive(Debug, Serialize, Deserialize, Clone)]
struct BNKStreamFrame {
    stream: String,
    data: BNKlineData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct BNKlineData {
    #[serde(rename = "e")]
    event_type: String,
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "k")]
    kline: BNKline,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct BNKline {
    #[serde(rename = "t")]
    start_time: i64,
    #[serde(rename = "T")]
    end_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "i")]
    interval: String,
    #[serde(rename = "o")]
    open: String,
    #[serde(rename = "c")]
    close: String,
    #[serde(rename = "h")]
    high: String,
    #[serde(rename = "l")]
    low: String,
    #[serde(rename = "v")]
    volume: String,
    #[serde(rename = "n")]
    number_of_trades: i32,
    #[serde(rename = "x")]
    is_closed: bool,
}

pub struct BNClient {
    pub ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub tx: Sender<CPTKline>,
}

impl BNClient {

    pub async fn connect(tx: Sender<CPTKline>) -> anyhow::Result<Self> {
        let url = format!("wss://stream.binance.com:9443/stream");
        let (ws_stream, _) = connect_async(url).await?;
        info!("Connected to Binance");

        Ok(BNClient {
            ws_stream,
            tx,
        })
    }

    pub async fn subscribe_kline(&mut self, codes: Vec<String>) -> anyhow::Result<()> {
        let subs = json!({
            "method": "SUBSCRIBE",
            "params": codes.iter().map(|code| format!("{}@kline_1s", code)).collect::<Vec<String>>(),
            "id": 1
        });
        self.ws_stream.send(Message::Text(subs.to_string())).await?;
        info!("Subscribed to Binance");
        Ok(())
    }

    pub async fn on_recv(&mut self) -> anyhow::Result<()>
    {
        while let Some(message) = self.ws_stream.next().await {
            match message {
                Ok(Message::Text(text)) => match serde_json::from_str::<BNKStreamFrame>(&text) {
                    Ok(frame) => {
                        let kline_data = frame.data;
                        if !kline_data.kline.is_closed {
                            continue;
                        }
                        let k = kline_data.kline;
                        let now_ts_ns = chrono::Local::now().timestamp_nanos_opt().unwrap_or(0);

                        let mut kline = CPTKline {
                            exchange: EXCHID_BINANCE,
                            code: [0; 16],
                            open_time_ms: k.start_time as u64,
                            interval: 1,
                            local_ts_ns: now_ts_ns as u64,
                            open: k.open.parse().unwrap_or(0.0),
                            close: k.close.parse().unwrap_or(0.0),
                            high: k.high.parse().unwrap_or(0.0),
                            low: k.low.parse().unwrap_or(0.0),
                        };
                        write_fixed(&mut kline.code, &k.symbol);
                        if let Err(err) = self.tx.try_send(&kline) {
                            bail!("共享内存写入失败: {err:?}");
                        }
                    },
                    Err(e) => {
                        if text.contains("\"result\":null") {
                            // 订阅成功的响应，不做处理
                            continue;
                        }
                        error!("Failed to parse message: {}, error: {}", text, e);
                    }
                },
                Ok(Message::Ping(ping)) => {
                    // info!("Received Ping from Binance");
                    self.ws_stream.send(Message::Pong(ping)).await?;
                },
                Err(e) => {
                    error!("WebSocket error: {}", e);
                },
                _ => {}
            }

        }

        Ok(())
    }
}
