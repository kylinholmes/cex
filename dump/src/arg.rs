#[derive(Debug, Clone, Copy, clap::ValueEnum, PartialEq, Eq)]
pub enum DataType {
    Kline,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputType {
    Stdout,
    File,
}

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Format {
    Json,
    Csv,
    Parquet,
}
