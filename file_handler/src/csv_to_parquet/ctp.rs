use polars::frame::DataFrame;
use polars::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::BufWriter;


pub fn read_data_frame_from_csv(csv_file_path: &Path) -> DataFrame {
    CsvReader::from_path(csv_file_path)
        .expect("Cannot open file.")
        .has_header(true)
        .finish()
        .unwrap()
}


pub fn csv_to_parquet(filename: &str) {
    // Open the CSV file
    let csv_file: &Path = Path::new(filename); //"../../MOCK_DATA.csv");
    let mut df: DataFrame = read_data_frame_from_csv(csv_file);

    // Write DataFrame
    let file = File::create("output.parquet").expect("could not create file");
    let bfw = BufWriter::new(file);
    let pw = ParquetWriter::new(bfw).with_compression(ParquetCompression::Snappy);
    let _ = pw.finish(&mut df);

}