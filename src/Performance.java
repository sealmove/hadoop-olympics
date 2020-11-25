/* Anastasia Tsilepi 2022 2015 00179 dit15179
 * Stefanos Mandalas 2022 2017 00107 dit17107 */

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.DataInput;
import java.io.DataOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Performance {
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
  public static class CustomWritable
  implements WritableComparable<CustomWritable> {
    private final IntWritable id;
    private final Text name;
    private final IntWritable sex;

    public CustomWritable() {
      this.id = new IntWritable(0);
      this.name = new Text();
      this.sex = new IntWritable(0);
    }

    public CustomWritable(int id, String name, Sex sex) {
      this.id = new IntWritable(id);
      this.name = new Text(name);
      this.sex = new IntWritable(sex.ordinal());
    }

    public void readFields(DataInput in) throws IOException {
      id.readFields(in);
      name.readFields(in);
      sex.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
      id.write(out);
      name.write(out);
      sex.write(out);
    }

    // This is necessary because reducer needs to know how to order keys
    public int compareTo(CustomWritable cw) { return id.compareTo(cw.id); }

    @Override
    public String toString() {
      return id + "\t" + name + "\t" + Sex.values()[sex.get()];
    }
  }

  public static class PerformanceMapper
  extends Mapper<Object, Text, CustomWritable, IntWritable> {
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

          // Value
          String medalStr = row[14];
          int medal = medalStr.equals("Gold") ? 1 : 0;

          context.write(
            new CustomWritable(id, name, sex),
            new IntWritable(medal)
          );
        }
      } catch (CsvValidationException cve) {}
      csvReader.close();
    }
  }
  
  public static class PerformanceReducer
  extends Reducer<CustomWritable, IntWritable, CustomWritable, IntWritable> {
    public void reduce(CustomWritable key, Iterable<IntWritable> values,
    Context context) throws IOException, InterruptedException {
      int golds = 0;
      for (IntWritable val : values)
        golds += val.get();
      context.write(key, new IntWritable(golds));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs =
      new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length < 2) {
      System.err.println("Usage: performance <in> [<in>...] <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Gold medal count");
    job.setJarByClass(Performance.class);
    job.setMapperClass(PerformanceMapper.class);
    job.setCombinerClass(PerformanceReducer.class);
    job.setReducerClass(PerformanceReducer.class);
    job.setOutputKeyClass(CustomWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJarByClass(Performance.class);
    for (int i = 0; i < otherArgs.length - 1; ++i)
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    job.waitForCompletion(true);
  }
}