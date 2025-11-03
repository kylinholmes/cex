use std::time::Duration;
use cex_core::{CH_KLINE_V1, CHANNEL_CAP, CPTKline, ChannelError, create_channel};
use tokio::time::sleep;
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

    let (_, receiver) = match create_channel::<CPTKline>(CH_KLINE_V1, CHANNEL_CAP) {
        Ok(pair) => pair,
        Err(err) => {
            error!("Shm Create/Attach Err: {err:?}");
            return;
        }
    };
    info!("Shm Create/Attach Ok, {}", CH_KLINE_V1);

    loop {
        match receiver.next() {
            Ok(kline) => info!("{}", kline),
            Err(ChannelError::Empty) => {
                sleep(Duration::from_millis(10)).await;
                continue;
            }
            Err(err) => {
                error!("共享内存读取失败: {err:?}");
                break;
            }
        }
    }
}
