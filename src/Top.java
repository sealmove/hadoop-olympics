/* Anastasia Tsilepi 2022 2015 00179 dit15179
 * Stefanos Mandalas 2022 2017 00107 dit17107 */

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
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

public class Top {
  static int n = 1;
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

  public static Medal tryParseMedal(String s) {
    Medal result;
    try {
      result = Medal.valueOf(s);
    } catch (IllegalArgumentException iae) {
      result = Medal.NA;
    }
    return result;
  }

  enum Sex {NA, M, F}
  enum Medal {NA, Gold, Silver, Bronze}

  public static class AthleteWritable
  implements WritableComparable<AthleteWritable> {
    private final IntWritable id;
    private final Text name;
    private final IntWritable sex;
    private final IntWritable age;
    private final Text team;
    private final Text games;
    private final Text sport;

    public IntWritable getId() { return id; }
    public Text getName() { return name; }
    public IntWritable getSex() { return sex; }
    public IntWritable getAge() { return age; }
    public Text getTeam() { return team; }
    public Text getGames() { return games; }
    public Text getSport() { return sport; }

    public AthleteWritable() {
      this.id = new IntWritable(0);
      this.name = new Text();
      this.sex = new IntWritable(0);
      this.age = new IntWritable(0);
      this.team = new Text();
      this.games = new Text();
      this.sport = new Text();
    }

    public AthleteWritable(int id, String name, Sex sex, int age, String team,
                           String games, String sport) {
      this.id = new IntWritable(id);
      this.name = new Text(name);
      this.sex = new IntWritable(sex.ordinal());
      this.age = new IntWritable(age);
      this.team = new Text(team);
      this.games = new Text(games);
      this.sport = new Text(sport);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      id.readFields(in);
      name.readFields(in);
      sex.readFields(in);
      age.readFields(in);
      team.readFields(in);
      games.readFields(in);
      sport.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      id.write(out);
      name.write(out);
      sex.write(out);
      age.write(out);
      team.write(out);
      games.write(out);
      sport.write(out);
    }

    // Defines defines the way data will be partitioned and ordered
    @Override
    public int compareTo(AthleteWritable aw) {
      if (id.equals(aw.id)) {
        return games.compareTo(aw.games);
      } else {
        return id.compareTo(aw.id);
      }
    }

    @Override
    public String toString() {
      String[] entry = {
        id.toString(),
        name.toString(),
        Sex.values()[sex.get()].toString(),
        age.toString(),
        team.toString(),
        sport.toString(),
        games.toString()
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

  public static class MedalsWritable implements Writable {
    private final IntWritable golds;
    private final IntWritable silvers;
    private final IntWritable bronzes;
    private final IntWritable total;

    public IntWritable getGolds() { return golds; }
    public IntWritable getSilvers() { return silvers; }
    public IntWritable getBronzes() { return bronzes; }
    public IntWritable getTotal() { return total; }

    public MedalsWritable() {
      this.golds = new IntWritable(0);
      this.silvers = new IntWritable(0);
      this.bronzes = new IntWritable(0);
      this.total = new IntWritable(0);
    }

    public MedalsWritable(int golds, int silvers, int bronzes, int total) {
      this.golds = new IntWritable(golds);
      this.silvers = new IntWritable(silvers);
      this.bronzes = new IntWritable(bronzes);
      this.total = new IntWritable(total);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      golds.readFields(in);
      silvers.readFields(in);
      bronzes.readFields(in);
      total.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      golds.write(out);
      silvers.write(out);
      bronzes.write(out);
      total.write(out);
    }

    @Override
    public String toString() {
      String[] entry = {
        golds.toString(),
        silvers.toString(),
        bronzes.toString(),
        total.toString()
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

  public static class ChampionWritable
  implements WritableComparable<ChampionWritable> {
    private IntWritable rank;
    private final AthleteWritable athlete;
    private final MedalsWritable medals;

    public AthleteWritable getAthlete() { return athlete; }
    public MedalsWritable getMedals() { return medals; }
    public IntWritable getRank() { return rank; }

    public void setRank(int rank) { this.rank = new IntWritable(rank); }

    public ChampionWritable() {
      rank = new IntWritable(0);
      athlete = new AthleteWritable();
      medals = new MedalsWritable();
    }

    public ChampionWritable(int id, String name, Sex sex, int age, String team,
                            String games, String sport, int golds, int silvers,
                            int bronzes, int total) {
      rank = new IntWritable(0);
      athlete = new AthleteWritable(id, name, sex, age, team, games, sport);
      medals = new MedalsWritable(golds, silvers, bronzes, total);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      rank.readFields(in);
      athlete.readFields(in);
      medals.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
      rank.write(out);
      athlete.write(out);
      medals.write(out);
    }

    // Defines defines the way data will be partitioned and ordered
    @Override
    public int compareTo(ChampionWritable cw) {
      int cmp = medals.getGolds().compareTo(cw.getMedals().getGolds());
      if (cmp != 0) return -cmp;
      cmp = medals.getTotal().compareTo(cw.getMedals().getTotal());
      if (cmp != 0) return -cmp;
      cmp = athlete.getName().compareTo(cw.getAthlete().getName());
      if (cmp != 0) return cmp;
      cmp = athlete.getGames().compareTo(cw.getAthlete().getGames());
      if (cmp != 0) return cmp;
      return 0;
    }

    @Override
    public String toString() {
      return getRank() + " " +
             athlete.getName() + " " +
             Sex.values()[athlete.getSex().get()] + " " +
             athlete.getAge() + " " +
             athlete.getTeam() + " " +
             athlete.getSport() + " " +
             athlete.getGames() + " " +
             medals.getGolds() + " " +
             medals.getSilvers() + " " +
             medals.getBronzes() + " " +
             medals.getTotal();
    }
  }

  public static class GoldMedalCountMapper
  extends Mapper<Object, Text, AthleteWritable, IntWritable> {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
      StringReader stringReader = new StringReader(value.toString());
      CSVReader csvReader = new CSVReader(stringReader);
      String[] row;
      try {
        while ((row = csvReader.readNext()) != null) {
          // Key
          int id = tryParseInt(row[0]);
          String name = row[1];
          Sex sex = tryParseSex(row[2]);
          int age = tryParseInt(row[3]);
          String team = row[6];
          String games = row[8];
          String sport = row[12];

          // Value
          Medal medal = tryParseMedal(row[14]);

          context.write(
            new AthleteWritable(id, name, sex, age, team, games, sport),
            new IntWritable(medal.ordinal())
          );
        }
      } catch (CsvValidationException cve) {}
      csvReader.close();
    }
  }

  public static class GoldMedalCountReducer
  extends Reducer<AthleteWritable, IntWritable, AthleteWritable, MedalsWritable> {
    public void reduce(AthleteWritable key, Iterable<IntWritable> values,
    Context context) throws IOException, InterruptedException {
      int golds = 0;
      int silvers = 0;
      int bronzes = 0;
      for (IntWritable val : values) {
        switch (Medal.values()[val.get()]) {
        case Gold:
          ++golds;
          break;
        case Silver:
          ++silvers;
          break;
        case Bronze:
          ++bronzes;
          break; 
        }
      }
      int total = golds + silvers + bronzes;

      context.write(key, new MedalsWritable(golds, silvers, bronzes, total));
    }
  }

  public static class RankingMapper
  extends Mapper<Object, Text, ChampionWritable, NullWritable> {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
      StringReader stringReader = new StringReader(value.toString());
      CSVReader csvReader = new CSVReader(stringReader);
      String[] row;
      try {
        while ((row = csvReader.readNext()) != null) {
          // Key
          int id = tryParseInt(row[0]);
          String name = row[1];
          Sex sex = tryParseSex(row[2]);
          int age = tryParseInt(row[3]);
          String team = row[4];
          String games = row[5];
          String sport = row[6];
          int golds = tryParseInt(row[7]);
          int silvers = tryParseInt(row[8]);
          int bronzes = tryParseInt(row[9]);
          int total = tryParseInt(row[10]);

          context.write(
            new ChampionWritable(id, name, sex, age, team, games, sport, golds,
                                 silvers, bronzes, total),
            NullWritable.get()
          );
        }
      } catch (CsvValidationException cve) {}
      csvReader.close();
    }
  }

  public static class RankingReducer
  extends Reducer<ChampionWritable, NullWritable, ChampionWritable, NullWritable> {
    public void reduce(ChampionWritable key, Iterable<NullWritable> values,
    Context context) throws IOException, InterruptedException {
      if (n <= 10) {
        for (NullWritable val : values) {
          key.setRank(n);
          context.write(key, val);
        }
      }
      ++n;
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    conf.set("mapreduce.output.textoutputformat.separator", ",");

    String[] hargs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (hargs.length < 2) {
      System.err.println("Usage: top <in> [<in>...] <out>");
      System.exit(2);
    }

    Job job1 = new Job(conf, "Gold medal count");
    job1.setJarByClass(Top.class);
    job1.setMapperClass(GoldMedalCountMapper.class);
    job1.setReducerClass(GoldMedalCountReducer.class);
    job1.setMapOutputKeyClass(AthleteWritable.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(AthleteWritable.class);
    job1.setOutputValueClass(MedalsWritable.class);
    FileInputFormat.addInputPath(job1, new Path(hargs[0]));
    FileOutputFormat.setOutputPath(job1, new Path("temp"));

    conf.set("mapreduce.output.textoutputformat.separator", " ");
    Job job2 = new Job(conf, "Ranking");
    job2.setJarByClass(Top.class);
    job2.setMapperClass(RankingMapper.class);
    job2.setReducerClass(RankingReducer.class);
    job2.setCombinerClass(RankingReducer.class);
    job2.setOutputKeyClass(ChampionWritable.class);
    job2.setOutputValueClass(NullWritable.class);
    FileInputFormat.addInputPath(job2, new Path("temp"));
    FileOutputFormat.setOutputPath(job2, new Path(hargs[1]));

    job1.waitForCompletion(true);
    job2.waitForCompletion(true);
  }
}