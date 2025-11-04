use log::{error, info};
use serde::{Deserialize, Serialize};
use cex_core::{COIN_TYPE_SPOT, CPTKline, EXCHID_OKX, Sender, write_fixed};

use futures_util::{SinkExt, StreamExt};
use serde_json::json;
use tokio::net::TcpStream;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async, tungstenite::protocol::Message};

/*
{
    "event": "subscribe",
    "arg": {
        "channel": "candle1s",
        "instId": "BTC-USDT"
    },
    "connId": "a76da401"
}

{
    "arg": {
        "channel": "candle1s",
        "instId": "BTC-USDT"
    },
    "data": [
        [
            "1762233412000",
            "106362.7",
            "106362.7",
            "106362.6",
            "106362.6",
            "0.0303294",
            "3225.914524232",
            "3225.914524232",
            "1"
        ]
    ]
}
*/


#[derive(Debug, Serialize, Deserialize)]
struct OKXStreamFrame {
    arg: OKXArg,
    data: Vec<[String; 9]>,
}

#[derive(Debug, Serialize, Deserialize)]
struct OKXArg {
    channel: String,
    #[serde(rename = "instId")]
    code: String,
}

pub struct OKXClient {
    pub ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    pub tx: Sender<CPTKline>,
}

impl OKXClient {
    pub async fn connect(tx: Sender<CPTKline>) -> anyhow::Result<Self> {
        let url = format!("wss://wseea.okx.com:8443/ws/v5/business");
        let (ws_stream, _) = connect_async(url).await?;
        info!("Connected to OKX");
        Ok(OKXClient { ws_stream, tx })
    }
    
    pub async fn subscribe_kline(&mut self, codes: Vec<String>) -> anyhow::Result<()> {
        let sub = json!({
            "op": "subscribe",
            "args": codes.iter().map(|code| {
                let code = if code.to_lowercase().contains("usdt") {
                    code.to_uppercase()
                } else {
                    format!("{}-USDT", code.to_uppercase())
                };
                json!({
                    "channel": "candle1s",
                    "instId": code,
                    "instType":"SPOT"
                })
            }).collect::<Vec<_>>(),
        });

        self.ws_stream.send(Message::Text(sub.to_string())).await?;
        info!("Subscribed to OKX");
        Ok(())
    }

    pub async fn on_recv(&mut self) -> anyhow::Result<()> {
        while let Some(message) = self.ws_stream.next().await {
            match message {
                Ok(Message::Text(msg)) => match serde_json::from_str::<OKXStreamFrame>(&msg) {
                    Ok(frame) => {
                        let code = frame.arg.code.split("-USDT").next().unwrap_or(&frame.arg.code);
                        let local_ts_ms = chrono::Local::now().timestamp_millis();
                        for data in frame.data {
                            let open_time_ms = data[0].parse::<i64>().unwrap_or(0);
                            let mut kline = CPTKline {
                                code: [0; 16],
                                open: data[1].parse().unwrap_or(0.0),
                                close: data[2].parse().unwrap_or(0.0),
                                high: data[3].parse().unwrap_or(0.0),
                                low: data[4].parse().unwrap_or(0.0),
                                exchange: EXCHID_OKX,
                                data_type: COIN_TYPE_SPOT,
                                open_time_ms: open_time_ms as u64,
                                interval: data[8].parse().unwrap_or(0),
                                local_ts_ms: local_ts_ms as u64,
                            };
                            write_fixed(&mut kline.code, code);
                            self.tx.send(&kline)?;
                        }
                    },
                    Err(_) => {
                        error!("Failed to parse: {:?}", msg);
                    }
                },
                Ok(Message::Ping(ping)) => {
                    self.ws_stream.send(Message::Pong(ping)).await?;
                },
                Err(e) => {
                    error!("WebSocket Error: {:?}", e);
                },
                _ => { }
            }
        }
        Ok(())
    }

}

