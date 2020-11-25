/* Anastasia Tsilepi 2022 2015 00179 dit15179
 * Stefanos Mandalas 2022 2017 00107 dit17107 */

import java.io.IOException;
import java.io.StringReader;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.Writable;
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

public class Top {
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
    private final Text sport;
    private final Text games;

    public AthleteWritable() {
      this.id = new IntWritable(0);
      this.name = new Text();
      this.sex = new IntWritable(0);
      this.age = new IntWritable(0);
      this.team = new Text();
      this.sport = new Text();
      this.games = new Text();
    }

    public AthleteWritable(int id, String name, Sex sex, int age, String team,
                          String sport, String games) {
      this.id = new IntWritable(id);
      this.name = new Text(name);
      this.sex = new IntWritable(sex.ordinal());
      this.age = new IntWritable(age);
      this.team = new Text(team);
      this.sport = new Text(sport);
      this.games = new Text(games);
    }

    public void readFields(DataInput in) throws IOException {
      id.readFields(in);
      name.readFields(in);
      sex.readFields(in);
      age.readFields(in);
      team.readFields(in);
      sport.readFields(in);
      games.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      id.write(out);
      name.write(out);
      sex.write(out);
      age.write(out);
      team.write(out);
      sport.write(out);
      games.write(out);
    }

    // This is necessary because reducer needs to know how to order keys
    public int compareTo(AthleteWritable cw) {
      if (id.compareTo(cw.id) == 0) {
        return games.compareTo(cw.games);
      } else {
        return id.compareTo(cw.id);
      }
    }

    @Override
    public String toString() {
      return name + "\t" + Sex.values()[sex.get()] + "\t" + age + "\t" + games;
    }
  }

  public static class MedalsWritable implements Writable {
    private final IntWritable golds;
    private final IntWritable silvers;
    private final IntWritable bronzes;
    private final IntWritable total;

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

    public void readFields(DataInput in) throws IOException {
      golds.readFields(in);
      silvers.readFields(in);
      bronzes.readFields(in);
      total.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      golds.write(out);
      silvers.write(out);
      bronzes.write(out);
      total.write(out);
    }

    @Override
    public String toString() {
      return golds + "\t" + silvers + "\t" + bronzes + "\t" + total;
    }
  }

  public static class TopMapper
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

  public static class TopReducer
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

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] args = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (args.length < 2) {
      System.err.println("Usage: top <in> [<in>...] <out>");
      System.exit(2);
    }
    
    Job job1 = new Job(conf, "Gold medal count");
    job1.setJarByClass(Top.class);
    job1.setMapperClass(TopMapper.class);
    job1.setReducerClass(TopReducer.class);
    job1.setMapOutputKeyClass(AthleteWritable.class);
    job1.setMapOutputValueClass(IntWritable.class);
    job1.setOutputKeyClass(AthleteWritable.class);
    job1.setOutputValueClass(MedalsWritable.class);
    job1.setJarByClass(Top.class);

    //Job job2 = new Job(conf, "Ranking");

    for (int i = 0; i < args.length - 1; ++i)
      FileInputFormat.addInputPath(job1, new Path(args[i]));
    FileOutputFormat.setOutputPath(job1, new Path(args[args.length - 1]));

    job1.waitForCompletion(true);
    //job2.waitForCompletion(true);
  }
}