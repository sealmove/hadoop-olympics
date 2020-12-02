define CSVLoader org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
define CSVWriter org.apache.pig.piggybank.storage.CSVExcelStorage(' ', 'NO_MULTILINE', 'UNIX');

inp = load 'input' using CSVLoader()
  as (id:int, name:chararray, sex:chararray, age:int, height:int, weight:int,
      team:chararray, noc:chararray, games:chararray, year:int,
      season:chararray, city:chararray, sport:chararray, event:chararray,
      medal:chararray);
