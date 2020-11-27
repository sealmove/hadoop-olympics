/* Anastasia Tsilepi 2022 2015 00179 dit15179
 * Stefanos Mandalas 2022 2017 00107 dit17107 */

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.StringWriter;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

// https://sourceforge.net/projects/opencsv/
import com.opencsv.CSVReader;
import com.opencsv.CSVWriter;
import com.opencsv.exceptions.CsvValidationException;

public class Female {
  static HashMap<Text, Integer> recordCounts = new HashMap<>();
  static HashMap<Text, Integer> lastTeamSizes = new HashMap<>();

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
  public static class TeamPartWritable
  implements WritableComparable<TeamPartWritable> {
    private final Text team;
    private final Text noc;
    private final Text games;
    private final IntWritable year;

    public TeamPartWritable() {
      this.team = new Text();
      this.noc = new Text();
      this.games = new Text();
      this.year = new IntWritable(0);
    }

    public TeamPartWritable(String team, String noc, String games, int year) {
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

    // Defines the way data will be partitioned and ordered
    @Override
    public int compareTo(TeamPartWritable tpw) {
      int cmp = year.compareTo(tpw.year);
      if (cmp != 0) return cmp;
      cmp = team.compareTo(tpw.team);
      if (cmp != 0) return cmp;
      cmp = games.compareTo(tpw.games);
      if (cmp != 0) return cmp;
      return 0;
    }

    @Override
    public String toString() {
      String[] entry = {
        games.toString(),
        team.toString(),
        noc.toString()
      };
      StringWriter stringWriter = new StringWriter();
      CSVWriter csvWriter = new CSVWriter(stringWriter);
      try {
        csvWriter.writeNext(entry);
        csvWriter.close();
      } catch (IOException ioe) {}
      String result = stringWriter.toString();
      return result.substring(0, result.length() - 1);
    }
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
    public String toString() {
      String[] entry = {
        num.toString(),
        sport.toString()
      };
      StringWriter stringWriter = new StringWriter();
      CSVWriter csvWriter = new CSVWriter(stringWriter);
      try {
        csvWriter.writeNext(entry);
        csvWriter.close();
      } catch (IOException ioe) {}
      String result = stringWriter.toString();
      return result.substring(0, result.length() - 1);
    }
  }

  public static class TeamAthletesWritable
  implements WritableComparable<TeamAthletesWritable> {
    private final Text games;
    private final Text team;
    private final Text noc;
    private final IntWritable athletes;
    private final Text sport;

    public Text getGames() { return games; }
    public Text getTeam() { return team; }
    public Text getNoc() { return noc; }
    public IntWritable getAthletes() { return athletes; }
    public Text getSport() { return sport; }

    public TeamAthletesWritable() {
      this.games = new Text();
      this.team = new Text();
      this.noc = new Text();
      this.athletes = new IntWritable(0);
      this.sport = new Text();
    }

    public TeamAthletesWritable(String games, String team, String noc,
                            int athletes, String sport) {
      this.games = new Text(games);
      this.team = new Text(team);
      this.noc = new Text(noc);
      this.athletes = new IntWritable(athletes);
      this.sport = new Text(sport);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      games.readFields(in);
      team.readFields(in);
      noc.readFields(in);
      athletes.readFields(in);
      sport.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      games.write(out);
      team.write(out);
      noc.write(out);
      athletes.write(out);
      sport.write(out);
    }

    // Defines the way data will be partitioned and ordered
    @Override
    public int compareTo(TeamAthletesWritable taw) {
      int cmp = games.compareTo(taw.games);
      if (cmp != 0) return cmp;
      cmp = athletes.compareTo(taw.athletes);
      if (cmp != 0) return -cmp;
      cmp = team.compareTo(taw.team);
      if (cmp != 0) return cmp;
      return 0;
    }

    @Override
    public String toString() {
      return games + " " + team + " " + noc + " " + athletes + " " + sport;
    }
  }

  public static class TeamPartMapper
  extends Mapper<Object, Text, TeamPartWritable, CustomWritable> {
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
            new TeamPartWritable(team, noc, games, year),
            new CustomWritable(id, sport)
          );
        }
      } catch (CsvValidationException cve) {}
      csvReader.close();
    }
  }

  public static class TeamPartReducer
  extends Reducer<TeamPartWritable, CustomWritable,
                  TeamPartWritable, CustomWritable> {
    public void reduce(TeamPartWritable key, Iterable<CustomWritable> values,
    Context context) throws IOException, InterruptedException {
      HashSet<IntWritable> ids = new HashSet<>();
      HashMap<Text, Integer> sportCounts = new HashMap<>();

      for (CustomWritable val : values) {
        ids.add(val.getNum());

        Text sport = val.getSport();
        int i = sportCounts.containsKey(sport) ? sportCounts.get(sport) : 0;
        sportCounts.put(sport, i + 1);
      }

      // Sport with the maximum count
      Map.Entry<Text, Integer> topSport = null;
      for (Map.Entry<Text, Integer> entry : sportCounts.entrySet())
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

  public static class RankingMapper
  extends Mapper<Object, Text, TeamAthletesWritable, NullWritable> {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
      StringReader stringReader = new StringReader(value.toString());
      CSVReader csvReader = new CSVReader(stringReader);
      String[] row;
      try {
        while ((row = csvReader.readNext()) != null) {
          String games = row[0];
          String team = row[1];
          String noc = row[2];
          int athletes = tryParseInt(row[3]);
          String sport = row[4];

          context.write(
            new TeamAthletesWritable(games, team, noc, athletes, sport),
            NullWritable.get()
          );
        }
      } catch (CsvValidationException cve) {}
      csvReader.close();
    }
  }

  public static class RankingReducer
  extends Reducer<TeamAthletesWritable, NullWritable,
                  TeamAthletesWritable, NullWritable> {
    public void reduce(TeamAthletesWritable key, Iterable<NullWritable> values,
    Context context) throws IOException, InterruptedException {
      for (NullWritable val : values) {
        Text games = key.getGames();
        int athletes = key.getAthletes().get();

        int i = recordCounts.containsKey(games) ? recordCounts.get(games) : 0;
        recordCounts.put(games, i + 1);

        if (recordCounts.get(games) <= 3 ||
            lastTeamSizes.get(games) == athletes) {
          context.write(key, val);
          lastTeamSizes.put(games, athletes);
        }
      }
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");
    String[] hargs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (hargs.length < 2) {
      System.err.println("Usage: performance <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job1 = new Job(conf, "Count athletes per games-team pairs");
    job1.setJarByClass(Female.class);
    job1.setMapperClass(TeamPartMapper.class);
    job1.setReducerClass(TeamPartReducer.class);
    job1.setOutputKeyClass(TeamPartWritable.class);
    job1.setOutputValueClass(CustomWritable.class);
    FileInputFormat.addInputPath(job1, new Path(hargs[0]));
    FileOutputFormat.setOutputPath(job1, new Path("temp"));

    conf.set("mapreduce.output.textoutputformat.separator", " ");
    Job job2 = new Job(conf, "Rank teams on number of athletes per game");
    job2.setJarByClass(Female.class);
    job2.setMapperClass(RankingMapper.class);
    job2.setReducerClass(RankingReducer.class);
    job2.setCombinerClass(RankingReducer.class);
    job2.setOutputKeyClass(TeamAthletesWritable.class);
    job2.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job2, new Path("temp"));
    FileOutputFormat.setOutputPath(job2, new Path(hargs[1]));

    job1.waitForCompletion(true);
    job2.waitForCompletion(true);
  }
}