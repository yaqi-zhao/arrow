#include <arrow/api.h>
#include <arrow/csv/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <iostream>
#include <dirent.h>
#include <string>
// (Doc section: Includes)

// (Doc section: GenInitialFile)
arrow::Status GenInitialFile() {
  // Make a couple 8-bit integer arrays and a 16-bit integer array -- just like
  // basic Arrow example.
  arrow::Int8Builder int8builder;
  int8_t days_raw[5] = {1, 12, 17, 23, 28};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(days_raw, 5));
  std::shared_ptr<arrow::Array> days;
  ARROW_ASSIGN_OR_RAISE(days, int8builder.Finish());

  int8_t months_raw[5] = {1, 3, 5, 7, 1};
  ARROW_RETURN_NOT_OK(int8builder.AppendValues(months_raw, 5));
  std::shared_ptr<arrow::Array> months;
  ARROW_ASSIGN_OR_RAISE(months, int8builder.Finish());

  arrow::Int16Builder int16builder;
  int16_t years_raw[5] = {1990, 2000, 1995, 2000, 1995};
  ARROW_RETURN_NOT_OK(int16builder.AppendValues(years_raw, 5));
  std::shared_ptr<arrow::Array> years;
  ARROW_ASSIGN_OR_RAISE(years, int16builder.Finish());

  // Get a vector of our Arrays
  std::vector<std::shared_ptr<arrow::Array>> columns = {days, months, years};

  // Make a schema to initialize the Table with
  std::shared_ptr<arrow::Field> field_day, field_month, field_year;
  std::shared_ptr<arrow::Schema> schema;

  field_day = arrow::field("Day", arrow::int8());
  field_month = arrow::field("Month", arrow::int8());
  field_year = arrow::field("Year", arrow::int16());

  schema = arrow::schema({field_day, field_month, field_year});
  // With the schema and data, create a Table
  std::shared_ptr<arrow::Table> table;
  table = arrow::Table::Make(schema, columns);

  // Write out test files in IPC, CSV, and Parquet for the example to use.
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.arrow"));
  ARROW_ASSIGN_OR_RAISE(std::shared_ptr<arrow::ipc::RecordBatchWriter> ipc_writer,
                        arrow::ipc::MakeFileWriter(outfile, schema));
  ARROW_RETURN_NOT_OK(ipc_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(ipc_writer->Close());

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.csv"));
  ARROW_ASSIGN_OR_RAISE(auto csv_writer,
                        arrow::csv::MakeCSVWriter(outfile, table->schema()));
  ARROW_RETURN_NOT_OK(csv_writer->WriteTable(*table));
  ARROW_RETURN_NOT_OK(csv_writer->Close());

  ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_in.parquet"));
  PARQUET_THROW_NOT_OK(
      parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), outfile, 5));

  return arrow::Status::OK();
}
// (Doc section: GenInitialFile)

// (Doc section: RunMain)
arrow::Status RunMain() {
  // (Doc section: RunMain)
  // (Doc section: Gen Files)
  // Generate initial files for each format with a helper function -- don't worry,
  // we'll also write a table in this example.
   //ARROW_RETURN_NOT_OK(GenInitialFile());
  // (Doc section: Gen Files)

  // (Doc section: ReadableFile Definition)
  // First, we have to set up a ReadableFile object, which just lets us point our
  // readers to the right data on disk. We'll be reusing this object, and rebinding
  // it to multiple files throughout the example.
  std::shared_ptr<arrow::io::ReadableFile> infile;
  std::shared_ptr<arrow::io::FileOutputStream> outfile;
  std::string dirname;
    DIR *dp;
    struct dirent *dirp;
   std:: cout << "Please input a directory: ";
    std::cin >> dirname;
     std::string outdirname="/mnt/tpch100_parquet_qpl/"+dirname;
    dirname="/mnt/tpch100_parquet_qpl/"+dirname;

   

    if((dp = opendir(dirname.c_str())) == NULL)
    {
        std::cout << "Can't open " << dirname << std::endl;
    }
    while((dirp = readdir(dp)) != NULL)
    {
      if((dirp->d_name)[0]=='2'){

  ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open(dirname+"/"+dirp->d_name));
  //ARROW_ASSIGN_OR_RAISE(infile, arrow::io::ReadableFile::Open("p.parquet"));
  // (Doc section: Parquet Read Open)
  // (Doc section: Parquet FileReader)
  std::unique_ptr<parquet::arrow::FileReader> reader;
  // (Doc section: Parquet FileReader)
  // (Doc section: Parquet OpenFile)
  // Note that Parquet's OpenFile() takes the reader by reference, rather than returning
  // a reader.
  PARQUET_THROW_NOT_OK(
      parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));
  // (Doc section: Parquet OpenFile)

  // (Doc section: Parquet Read)
  std::shared_ptr<arrow::Table> parquet_table;
  // Read the table.
  PARQUET_THROW_NOT_OK(reader->ReadTable(&parquet_table));
  // (Doc section: Parquet Read)

  // (Doc section: Parquet Write)
  // Parquet writing does not need a declared writer object. Just get the output
  // file bound, then pass in the table, memory pool, output, and chunk size for
  // breaking up the Table on-disk.
  //ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open(outdirname+"/"+((std::string)dirp->d_name)));
  //ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out_p.parquet"));

  //std::shared_ptr<WriterProperties> column_write_props;
/*   PARQUET_THROW_NOT_OK(parquet::arrow::WriteTable(
    *parquet_table, arrow::default_memory_pool(), outfile, 1024));  */
   ARROW_ASSIGN_OR_RAISE(outfile, arrow::io::FileOutputStream::Open("test_out.csv"));
  // The CSV writer has simpler defaults, review API documentation for more complex usage.
  ARROW_ASSIGN_OR_RAISE(auto par_writer,
                        arrow::csv::MakeCSVWriter(outfile, parquet_table->schema()));
  ARROW_RETURN_NOT_OK(par_writer->WriteTable(*parquet_table));
  // Not necessary, but a safe practice.
  ARROW_RETURN_NOT_OK(par_writer->Close()); 
  // (Doc section: Parquet Write)
  // (Doc section: Return)
      }
    }
    closedir(dp);

  return arrow::Status::OK();
}
// (Doc section: Return)

// (Doc section: Main)
int main() {
  arrow::Status st = RunMain();
  if (!st.ok()) {
    std::cerr << st << std::endl;
    return 1;
  }
  return 0;
}