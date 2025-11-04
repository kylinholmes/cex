
use cex_core::{CPTKline, read_fixed};
use anyhow::Result;
use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Write},
    path::{Path, PathBuf},
    sync::Arc,
};

use parquet::{
    basic::Compression,
    data_type::ByteArray,
    file::properties::WriterProperties,
    file::writer::SerializedFileWriter,
    schema::parser::parse_message_type,
};

use crate::schema::KlineRecord;

pub enum Sink {
    Json(JsonSink),
    Csv(CsvSink),
    Parquet(ParquetSink),
}

impl Sink {
    pub fn write_kline(&mut self, kline: &CPTKline) -> Result<()> {
        match self {
            Sink::Json(s) => s.write(kline),
            Sink::Csv(s) => s.write(kline),
            Sink::Parquet(s) => s.write(kline),
        }
    }

    pub fn finish(&mut self) -> Result<()> {
        match self {
            Sink::Json(s) => s.flush(),
            Sink::Csv(s) => s.flush(),
            Sink::Parquet(s) => s.flush(),
        }
    }
}

pub struct JsonSink {
    writer: Box<dyn Write + Send>,
}

impl JsonSink {
    pub fn new(target: OutputTarget) -> Result<Self> {
        Ok(Self {
            writer: target.into_writer()?,
        })
    }

    pub fn write(&mut self, kline: &CPTKline) -> Result<()> {
        let row: KlineRecord = kline.into();
        let record = serde_json::to_value(&row)?;
        serde_json::to_writer(&mut self.writer, &record)?;
        self.writer.write_all(b"\n")?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

pub struct CsvSink {
    writer: csv::Writer<Box<dyn Write + Send>>,
}

impl CsvSink {
    pub fn new(target: OutputTarget) -> Result<Self> {
        let (boxed_writer, header_written) = target.into_writer_with_header_flag()?;
        let mut csv_writer = csv::Writer::from_writer(boxed_writer);
        // write header only if the target was empty
        if !header_written {
            csv_writer.write_record(&["code","open","close","high","low", "open_time_ms", "local_ts_ms", "interval", "exchange_name"])?;
        }
        Ok(Self { writer: csv_writer })
    }

    pub fn write(&mut self, kline: &CPTKline) -> Result<()> {
        let row: KlineRecord = kline.into();
        self.writer.serialize(row)?;
        self.writer.flush()?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        Ok(())
    }
}

pub struct ParquetSink {
    path: PathBuf,
    buffer: Vec<CPTKline>,
    dirty: bool,
}

impl ParquetSink {
    pub fn new(path: PathBuf) -> Self {
        Self {
            path,
            buffer: Vec::new(),
            dirty: false,
        }
    }

    pub fn write(&mut self, kline: &CPTKline) -> Result<()> {
        self.buffer.push(*kline);
        self.dirty = true;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if !self.dirty || self.buffer.is_empty() {
            return Ok(());
        }
        write_parquet_file(&self.path, &self.buffer)?;
        self.dirty = false;
        Ok(())
    }
}




pub enum OutputTarget<'a> {
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
                    .expect(&format!("open output file {path}"));
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
                    .expect(&format!("open output file {path}"));
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



fn write_parquet_file(path: &Path, rows: &[CPTKline]) -> Result<()> {
    let file = File::create(path).expect(&format!("create parquet file {}", path.display()));
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
    let schema = parse_message_type(schema_str).expect("parse parquet schema");
    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let schema = Arc::new(schema);
    let mut writer = SerializedFileWriter::new(file, schema, props).expect("create parquet writer");

    {
        // take ownership of the row group instead of borrowing it
        let mut row_group = writer
            .next_row_group()
            .expect("create parquet row group");
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
        let local_ts_ms: Vec<i64> = rows.iter().map(|r| r.local_ts_ms as i64).collect();
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
                    .write_batch(&local_ts_ms, None, None)?,
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

    writer.close().expect("close parquet writer");
    Ok(())
}
