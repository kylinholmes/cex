use binance::Client;
use cex_core::{CH_KLINE_V1, CHANNEL_CAP, CPTKline, create_channel, unlink_channel};
use log::{error, info};
use tracing_subscriber;


#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .init();

    unlink_channel(CH_KLINE_V1).unwrap();

    let (sender, _) = match create_channel::<CPTKline>(CH_KLINE_V1, CHANNEL_CAP) {
        Ok(pair) => pair,
        Err(err) => {
            error!("Shm Create/Attach Err: {err:?}");
            return;
        }
    };
    info!("Shm Create/Attach Ok, {}", CH_KLINE_V1);

    let mut client = match Client::connect(sender).await {
        Ok(client) => client,
        Err(err) => {
            error!("Connect Binance WebSocket 失败: {err:?}");
            return;
        }
    };

    let codes = vec!["btcusdt".to_string(), "ethusdt".to_string()];

    if let Err(err) = client.subscribe_kline(codes).await {
        error!("Sub Failed: {err:?}");
    }

    if let Err(e) = client.on_recv().await {
        error!("Error: {e:?}");
    }


}
