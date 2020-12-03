DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
DEFINE CSVWriter org.apache.pig.piggybank.storage.CSVExcelStorage(' ', 'NO_MULTILINE', 'UNIX');

inp = LOAD 'input' USING CSVLoader()
      AS (id:int, name:chararray, sex:chararray, age:int, height:int,
          weight:int, team:chararray, noc:chararray, games:chararray, year:int,
          season:chararray, city:chararray, sport:chararray, event:chararray,
          medal:chararray);
data = FOREACH (FILTER inp BY sex == 'F')
       GENERATE games, team, noc, id;
agg = FOREACH (GROUP data BY (games, team, noc)) {
  unique = DISTINCT $1.id;
  GENERATE FLATTEN(group), COUNT(unique) as count;
}
ord = ORDER agg BY games, count DESC;
lim = FOREACH (GROUP ord BY games) {
  x = LIMIT $1 3;
  GENERATE FLATTEN(x);
}
out = ORDER lim BY games, count DESC;
STORE out INTO 'pig_output' USING CSVWriter();