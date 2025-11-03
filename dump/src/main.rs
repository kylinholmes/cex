use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
    thread::sleep,
    time::Duration,
};

use anyhow::{Context, Result};
use cex_core::{
    read_fixed, CPTKline, ChannelError, ReaderStart, Receiver, CH_KLINE_V1, EXCHID_BINANCE,
    EXCHID_OKX,
};
use clap::Parser;
use log::{error, info};
use parquet::{
    basic::Compression,
    data_type::ByteArray,
    file::properties::WriterProperties,
    file::writer::SerializedFileWriter,
    schema::parser::parse_message_type,
};
use serde_json::json;
use tracing_subscriber;

mod arg;
use arg::{DataType, Format};

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Args {
    /// Data type to dump (currently only Kline is supported)
    #[clap(value_enum, short, long, default_value_t = DataType::Kline)]
    r#type: DataType,

    /// Output format
    #[clap(value_enum, short = 'f', long, default_value_t = Format::Json)]
    format: Format,

    /// Output file, if none specified, defaults to stdout (Parquet requires a file)
    #[clap(short, long)]
    output: Option<String>,
}

fn main() {
    if let Err(err) = run() {
        error!("{err:?}");
    }
}

fn run() -> Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_file(true)
        .with_line_number(true)
        .with_target(false)
        .init();

    if args.r#type != DataType::Kline {
        error!("Only Kline dump is implemented currently");
        return Ok(());
    }

    let mut sink = build_sink(&args)?;
    let receiver =
        Receiver::<CPTKline>::open_with_mode(CH_KLINE_V1, ReaderStart::FromBeginning)
            .context("attach dump reader to shared channel")?;
    info!("Attached to shared memory channel {CH_KLINE_V1}");

    loop {
        match receiver.next() {
            Ok(kline) => {
                if let Err(err) = sink.write_kline(&kline) {
                    error!("Write output failed: {err:?}");
                    break;
                }

                match receiver.pop() {
                    Ok(_) => {}
                    Err(ChannelError::StaleCursor) | Err(ChannelError::NoPending) => {}
                    Err(err) => {
                        error!("Commit cursor failed: {err:?}");
                        break;
                    }
                }
            }
            Err(ChannelError::Empty) => {
                sleep(Duration::from_millis(10));
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

enum Sink {
    Json(JsonSink),
    Csv(CsvSink),
    Parquet(ParquetSink),
}

impl Sink {
    fn write_kline(&mut self, kline: &CPTKline) -> Result<()> {
        match self {
            Sink::Json(s) => s.write(kline),
            Sink::Csv(s) => s.write(kline),
            Sink::Parquet(s) => s.write(kline),
        }
    }

    fn finish(&mut self) -> Result<()> {
        match self {
            Sink::Json(s) => s.flush(),
            Sink::Csv(s) => s.flush(),
            Sink::Parquet(s) => s.flush(),
        }
    }
}

struct JsonSink {
    writer: Box<dyn Write + Send>,
}

impl JsonSink {
    fn new(target: OutputTarget) -> Result<Self> {
        Ok(Self {
            writer: target.into_writer()?,
        })
    }

    fn write(&mut self, kline: &CPTKline) -> Result<()> {
        let row: KlineRecord = kline.into();
        let record = json!({
            "exchange": row.exchange_name,
            "exchange_id": row.exchange_id,
            "code": row.code,
            "open_time_ms": row.open_time_ms,
            "interval": row.interval,
            "local_ts_ns": row.local_ts_ns,
            "open": row.open,
            "close": row.close,
            "high": row.high,
            "low": row.low,
        });
        serde_json::to_writer(&mut self.writer, &record)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

struct CsvSink {
    writer: Box<dyn Write + Send>,
    header_written: bool,
}

impl CsvSink {
    fn new(target: OutputTarget) -> Result<Self> {
        let (writer, header_written) = target.into_writer_with_header_flag()?;
        Ok(Self {
            writer,
            header_written,
        })
    }

    fn write(&mut self, kline: &CPTKline) -> Result<()> {
        if !self.header_written {
            self.writer.write_all(
                b"exchange,exchange_id,code,open_time_ms,interval,local_ts_ns,open,close,high,low\n",
            )?;
            self.header_written = true;
        }

        let row: KlineRecord = kline.into();
        writeln!(
            self.writer,
            "{},{},{},{},{},{},{},{},{},{}",
            row.exchange_name,
            row.exchange_id,
            row.code.as_str(),
            row.open_time_ms,
            row.interval,
            row.local_ts_ns,
            row.open,
            row.close,
            row.high,
            row.low
        )?;
        self.writer.flush()?;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

struct ParquetSink {
    path: PathBuf,
    buffer: Vec<CPTKline>,
    dirty: bool,
}

impl ParquetSink {
    fn new(path: PathBuf) -> Self {
        Self {
            path,
            buffer: Vec::new(),
            dirty: false,
        }
    }

    fn write(&mut self, kline: &CPTKline) -> Result<()> {
        self.buffer.push(*kline);
        self.dirty = true;
        Ok(())
    }

    fn flush(&mut self) -> Result<()> {
        if !self.dirty || self.buffer.is_empty() {
            return Ok(());
        }
        write_parquet_file(&self.path, &self.buffer)?;
        self.dirty = false;
        Ok(())
    }
}

fn build_sink(args: &Args) -> Result<Sink> {
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

enum OutputTarget<'a> {
    Stdout,
    File(&'a str),
}

impl<'a> OutputTarget<'a> {
    fn into_writer(self) -> Result<Box<dyn Write + Send>> {
        match self {
            OutputTarget::Stdout => Ok(Box::new(BufWriter::new(io::stdout()))),
            OutputTarget::File(path) => {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .with_context(|| format!("open output file {path}"))?;
                Ok(Box::new(BufWriter::new(file)))
            }
        }
    }

    fn into_writer_with_header_flag(self) -> Result<(Box<dyn Write + Send>, bool)> {
        match self {
            OutputTarget::Stdout => Ok((Box::new(BufWriter::new(io::stdout())), false)),
            OutputTarget::File(path) => {
                let file = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .with_context(|| format!("open output file {path}"))?;
                let is_empty = file.metadata()?.len() == 0;
                Ok((Box::new(BufWriter::new(file)), !is_empty))
            }
        }
    }
}

impl<'a> From<Option<&'a str>> for OutputTarget<'a> {
    fn from(value: Option<&'a str>) -> Self {
        match value {
            Some(path) => OutputTarget::File(path),
            None => OutputTarget::Stdout,
        }
    }
}

fn exchange_name(id: i32) -> &'static str {
    match id {
        EXCHID_BINANCE => "Binance",
        EXCHID_OKX => "OKX",
        _ => "Unknown",
    }
}

struct KlineRecord {
    exchange_id: i32,
    exchange_name: &'static str,
    code: String,
    open_time_ms: u64,
    interval: i64,
    local_ts_ns: u64,
    open: f64,
    close: f64,
    high: f64,
    low: f64,
}

impl From<&CPTKline> for KlineRecord {
    fn from(value: &CPTKline) -> Self {
        Self {
            exchange_id: value.exchange,
            exchange_name: exchange_name(value.exchange),
            code: read_fixed(&value.code),
            open_time_ms: value.open_time_ms,
            interval: value.interval,
            local_ts_ns: value.local_ts_ns,
            open: value.open,
            close: value.close,
            high: value.high,
            low: value.low,
        }
    }
}

fn write_parquet_file(path: &Path, rows: &[CPTKline]) -> Result<()> {
    let file = File::create(path).with_context(|| format!("create parquet file {}", path.display()))?;
    let schema_str = "
        message kline_schema {
            REQUIRED INT32 exchange;
            REQUIRED BINARY code (UTF8);
            REQUIRED INT64 open_time_ms;
            REQUIRED INT64 interval;
            REQUIRED INT64 local_ts_ns;
            REQUIRED DOUBLE open;
            REQUIRED DOUBLE close;
            REQUIRED DOUBLE high;
            REQUIRED DOUBLE low;
        }
    ";
    let schema = parse_message_type(schema_str).context("parse parquet schema")?;
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let schema = Arc::new(schema);
    let mut writer = Arc::new(SerializedFileWriter::new(file, schema, props).context("create parquet writer")?);

    {
        let mut row_group = Arc::get_mut(&mut writer)
            .unwrap()
            .next_row_group()
            .context("create parquet row group")?;
        let exchanges: Vec<i32> = rows.iter().map(|r| r.exchange).collect();
        let codes: Vec<ByteArray> = rows
            .iter()
            .map(|r| {
                let bytes = read_fixed(&r.code).into_bytes();
                ByteArray::from(bytes)
            })
            .collect();
        let open_time_ms: Vec<i64> = rows.iter().map(|r| r.open_time_ms as i64).collect();
        let interval: Vec<i64> = rows.iter().map(|r| r.interval).collect();
        let local_ts_ns: Vec<i64> = rows.iter().map(|r| r.local_ts_ns as i64).collect();
        let open: Vec<f64> = rows.iter().map(|r| r.open).collect();
        let close: Vec<f64> = rows.iter().map(|r| r.close).collect();
        let high: Vec<f64> = rows.iter().map(|r| r.high).collect();
        let low: Vec<f64> = rows.iter().map(|r| r.low).collect();

        let mut column_index = 0;
        while let Some(mut column) = row_group.next_column()? {
            match column_index {
                0 => column
                    .typed::<parquet::data_type::Int32Type>()
                    .write_batch(&exchanges, None, None)?,
                1 => column
                    .typed::<parquet::data_type::ByteArrayType>()
                    .write_batch(&codes, None, None)?,
                2 => column
                    .typed::<parquet::data_type::Int64Type>()
                    .write_batch(&open_time_ms, None, None)?,
                3 => column
                    .typed::<parquet::data_type::Int64Type>()
                    .write_batch(&interval, None, None)?,
                4 => column
                    .typed::<parquet::data_type::Int64Type>()
                    .write_batch(&local_ts_ns, None, None)?,
                5 => column
                    .typed::<parquet::data_type::DoubleType>()
                    .write_batch(&open, None, None)?,
                6 => column
                    .typed::<parquet::data_type::DoubleType>()
                    .write_batch(&close, None, None)?,
                7 => column
                    .typed::<parquet::data_type::DoubleType>()
                    .write_batch(&high, None, None)?,
                8 => column
                    .typed::<parquet::data_type::DoubleType>()
                    .write_batch(&low, None, None)?,
                _ => unreachable!("unexpected column index {}", column_index),
            };

            column.close()?;
            column_index += 1;
        }

        row_group.close()?;
    }

    // 假设 writer: Arc<SerializedFileWriter<File>>
    let writer = Arc::try_unwrap(writer).unwrap();
    writer.close().context("close parquet writer")?;
    Ok(())
}
