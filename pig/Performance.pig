DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
DEFINE CSVWriter org.apache.pig.piggybank.storage.CSVExcelStorage(' ', 'NO_MULTILINE', 'UNIX');

inp = LOAD 'input' USING CSVLoader()
      AS (id:int, name:chararray, sex:chararray, age:int, height:int,
          weight:int, team:chararray, noc:chararray, games:chararray, year:int,
          season:chararray, city:chararray, sport:chararray, event:chararray,
          medal:chararray);
ath = FOREACH inp GENERATE id, name, sex, (medal == 'Gold' ? 1 : 0) as golds;
grp = GROUP ath by (id, name, sex);
agg = FOREACH grp GENERATE FLATTEN(group), SUM($1.golds);
out = ORDER agg by id;
STORE out INTO 'pig_output' USING CSVWriter();