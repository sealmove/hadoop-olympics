/* Anastasia Tsilepi 2022 2015 00179 dit15179
 * Stefanos Mandalas 2022 2017 00107 dit17107 */

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.DataInput;
import java.io.DataOutput;


import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// https://sourceforge.net/projects/opencsv/
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class Female {
  // Utility methods
  public static int tryParseInt(String s) {
    int result;
    try {
      result = Integer.parseInt(s);
    } catch(NumberFormatException nfe) { result = 0; }
    return result;
  }

  public static Sex tryParseSex(String s) {
    Sex result;
    try {
      result = Sex.valueOf(s);
    } catch (IllegalArgumentException iae) {
      result = Sex.NA;
    }
    return result;
  }

  enum Sex {NA, M, F}
  public static class PartWritable
  implements WritableComparable<PartWritable> {
    private final Text team;
    private final Text noc;
    private final Text games;
    private final IntWritable year;

    public PartWritable() {
      this.team = new Text();
      this.noc = new Text();
      this.games = new Text();
      this.year = new IntWritable(0);
    }

    public PartWritable(String team, String noc, String games, int year) {
      this.team = new Text(team);
      this.noc = new Text(noc);
      this.games = new Text(games);
      this.year = new IntWritable(year);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      team.readFields(in);
      noc.readFields(in);
      games.readFields(in);
      year.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      team.write(out);
      noc.write(out);
      games.write(out);
      year.write(out);
    }

    // This is defines the way data will be partitioned and ordered
    @Override
    public int compareTo(PartWritable pw) {
      int cmp = year.compareTo(pw.year);
      if (cmp != 0) return cmp;
      cmp = team.compareTo(pw.team);
      if (cmp != 0) return cmp;
      cmp = games.compareTo(pw.games);
      if (cmp != 0) return cmp;
      return 0;
    }

    @Override
    public String toString() { return games + " " + team + " " + noc; }
  }

  // This class is used to represent both (id, sport) and (count, sport) pairs
  public static class CustomWritable implements Writable {
    private final IntWritable num;
    private final Text sport;

    public IntWritable getNum() { return num; }
    public Text getSport() { return sport; }

    public CustomWritable() {
      this.num = new IntWritable(0);
      this.sport = new Text();
    }

    public CustomWritable(int num, String sport) {
      this.num = new IntWritable(num);
      this.sport = new Text(sport);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      num.readFields(in);
      sport.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      num.write(out);
      sport.write(out);
    }
    
    @Override
    public String toString() { return num + " " + sport; }
  }

  public static class FemaleMapper
  extends Mapper<Object, Text, PartWritable, CustomWritable> {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
      StringReader stringReader = new StringReader(value.toString());
      CSVReader csvReader = new CSVReader(stringReader);
      String[] row;
      try {
        while ((row = csvReader.readNext()) != null) {
          Sex sex = tryParseSex(row[2]);
          if (sex != Sex.F) continue; // only interested in females

          // Key
          String team = row[6];
          String noc = row[7];
          String games = row[8];
          int year = tryParseInt(row[9]);

          // Value
          int id = tryParseInt(row[0]);
          if (id == 0) continue; // skip first row (titles)
          String sport = row[12];

          context.write(
            new PartWritable(team, noc, games, year),
            new CustomWritable(id, sport)
          );
        }
      } catch (CsvValidationException cve) {}
      csvReader.close();
    }
  }

  public static class FemaleReducer
  extends Reducer<PartWritable, CustomWritable, PartWritable, CustomWritable> {
    public void reduce(PartWritable key, Iterable<CustomWritable> values,
    Context context) throws IOException, InterruptedException {
      HashSet<IntWritable> ids = new HashSet<>();
      HashMap<Text, Integer> sportsCount = new HashMap<>();

      for (CustomWritable val : values) {
        ids.add(val.getNum());

        Text sport = val.getSport();
        int i = sportsCount.containsKey(sport) ? sportsCount.get(sport) : 0;
        sportsCount.put(sport, i + 1);
      }

      // Sport with the maximum count
      Map.Entry<Text, Integer> topSport = null;
      for (Map.Entry<Text, Integer> entry : sportsCount.entrySet())
        if (topSport == null || entry.getValue().compareTo(topSport.getValue()) > 0)
          topSport = entry;

      context.write(
        key,
        new CustomWritable(
          ids.size(), // Number of distinct ids
          topSport.getKey().toString()
        )
      );
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", " ");
    String[] hargs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (hargs.length < 2) {
      System.err.println("Usage: performance <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Gold medal count");
    job.setJarByClass(Female.class);
    job.setMapperClass(FemaleMapper.class);
    job.setReducerClass(FemaleReducer.class);
    job.setOutputKeyClass(PartWritable.class);
    job.setOutputValueClass(CustomWritable.class);
    FileInputFormat.addInputPath(job, new Path(hargs[0]));
    FileOutputFormat.setOutputPath(job, new Path(hargs[1]));
    job.waitForCompletion(true);
  }
}