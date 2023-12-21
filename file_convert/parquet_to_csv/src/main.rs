use polars::frame::DataFrame;
use polars::prelude::*;
use std::path::Path;
use std::fs::File;
use std::io::BufWriter;

fn read_data_frame_from_parquet(parquet_file_path: &Path) -> PolarsResult<DataFrame> {
    let r = File::open(parquet_file_path).unwrap();
    let reader = ParquetReader::new(r);
    reader.finish()
}

fn main() {

    // Open the Parquet file
    let parquet_file: &Path = Path::new("output.parquet");  // created in csv_to_parquet from MOCK_DATA.csv
    let mut df = read_data_frame_from_parquet(parquet_file).unwrap();

    // Write DataFrame
    let file = File::create("output.csv").expect("could not create file");
    let bfw = BufWriter::new(file);
    let mut w = CsvWriter::new(bfw).has_header(true);
    let _ = w.finish(&mut df);

}