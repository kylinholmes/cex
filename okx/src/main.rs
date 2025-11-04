/// Binance WebSocket 推送 -> 共享内存 ring channel 的演示程序：
/// - create_channel 会在共享内存已经存在时自动附着
/// - 一个异步任务发送数据，一个本地异步任务轮询并打印收到的数据
use std::time::Duration;

use okx::Client;
use cex_core::{CH_KLINE_V1, CHANNEL_CAP, CPTKline, ChannelError, create_channel, list_shm, unlink_channel};
use tokio::time::sleep;
use log::{error, info};
use tracing_subscriber;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .init();

    for shm_info in list_shm().unwrap() {
        info!("{:?}", shm_info);
    }

    unlink_channel(CH_KLINE_V1).unwrap();

    let (sender, receiver) = match create_channel::<CPTKline>(CH_KLINE_V1, CHANNEL_CAP) {
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
            error!("Connect OKX WebSocket 失败: {err:?}");
            return;
        }
    };

    let codes = vec!["btc".to_string(), "eth".to_string()];

    let recv_loop = async move {
        loop {
            match receiver.try_recv() {
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
    };

    let ws_loop = async move {
        if let Err(err) = client.subscribe_kline(codes).await {
            error!("订阅 Binance K线失败: {err:?}");
        }
    };

    tokio::pin!(ws_loop);
    tokio::pin!(recv_loop);

    tokio::select! {
        _ = &mut ws_loop => {}
        _ = &mut recv_loop => {}
    }
}
