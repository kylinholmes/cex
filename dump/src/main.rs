use std::path::PathBuf;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{Result, Context};
use cex_core::*;
use clap::Parser;
use log::{error, info};

use tracing_subscriber;
use dump_cli::arg::{DataType, Format};
use dump_cli::sink::{CsvSink, JsonSink, OutputTarget, ParquetSink, Sink};


#[derive(Parser, Debug)]
#[clap(author="Kylin", version, about, long_about = None)]
pub struct Args {
    /// Data type to dump
    #[clap(value_enum, short, long, default_value_t = DataType::Kline)]
    r#type: DataType,

    /// Output format
    #[clap(value_enum, short = 'f', long, default_value_t = Format::Json)]
    format: Format,

    /// Output file, if none specified, defaults to stdout (Parquet requires a file)
    #[clap(short, long)]
    output: Option<String>,
}


pub fn build_sink(args: &Args) -> Result<Sink> {
    match args.format {
        Format::Json => {
            let target = OutputTarget::from(args.output.as_deref());
            Ok(Sink::Json(JsonSink::new(target)?))
        }
        Format::Csv => {
            let target = OutputTarget::from(args.output.as_deref());
            Ok(Sink::Csv(CsvSink::new(target)?))
        }
        Format::Parquet => {
            let path = args
                .output
                .as_deref()
                .context("Parquet output requires --file <path>")?;
            Ok(Sink::Parquet(ParquetSink::new(PathBuf::from(path))))
        }
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .with_writer(std::io::stderr)
        .init();

    if args.r#type != DataType::Kline {
        error!("Only Kline dump is implemented currently");
        return Ok(());
    }

    let mut sink = build_sink(&args)?;
    let receiver =
        Receiver::<CPTKline>::open_with_mode(CH_KLINE_V1, ReaderStart::FromBeginning)
            .context("attach dump reader to shared channel")?;

    const MAX_MISS_CNT: usize = 1000;
    let mut miss_cnt = 0;

    loop {
        match receiver.next() {
            Ok(kline) => {
                miss_cnt = 0;
                if let Err(err) = sink.write_kline(&kline) {
                    error!("Write output failed: {err:?}");
                    break;
                }
            }
            Err(ChannelError::Empty) => {
                sleep(Duration::from_millis(10));
                miss_cnt += 1;
                if miss_cnt >= MAX_MISS_CNT {
                    info!("No new data for a while, exiting...");
                    break;
                }
            }
            Err(err) => {
                error!("Shared memory read error: {err:?}");
                break;
            }
        }
    }

    sink.finish()?;
    Ok(())
}


