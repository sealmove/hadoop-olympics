x = load 'input' using PigStorage(',') as
  (id:int, name:chararray, sex:chararray, age:int, height:int, weight:int,
   team:chararray, noc:chararray, games:chararray, year:int, season:chararray,
   city:chararray, sport:chararray, event:chararray, medal:chararray);
store x into 'pig_output' using PigStorage(' ');