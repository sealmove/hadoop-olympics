DEFINE CSVLoader org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER');
DEFINE CSVWriter org.apache.pig.piggybank.storage.CSVExcelStorage(' ', 'NO_MULTILINE', 'UNIX');

inp = LOAD 'input' USING CSVLoader()
      AS (id:int, name:chararray, sex:chararray, age:int, height:int,
          weight:int, team:chararray, noc:chararray, games:chararray, year:int,
          season:chararray, city:chararray, sport:chararray, event:chararray,
          medal:chararray);
exp = FOREACH inp
      GENERATE id, name, sex, age, team, games, sport,
              (CASE medal
               WHEN 'Gold' THEN (1,0,0,1)
               WHEN 'Silver' THEN (0,1,0,1)
               WHEN 'Bronze' THEN (0,0,1,1)
               ELSE (0,0,0,0) END) AS medals;
flt = FOREACH exp
      GENERATE id, name, sex, age, team, games, sport,
               FLATTEN(medals) AS (gold, silver, bronze, total);
grp = GROUP flt BY (id, name, sex, age, team, games, sport);
agg = FOREACH grp
      GENERATE FLATTEN(group), SUM($1.gold) as gold, SUM($1.silver) as silver,
               SUM($1.bronze) as bronze, SUM($1.total) as total;
rnk = RANK agg BY gold DESC, total DESC, name ASC;
top = FILTER rnk BY $0 <= 10;
out = FOREACH top GENERATE $0, $2 ..;
STORE out INTO 'pig_output' USING CSVWriter();