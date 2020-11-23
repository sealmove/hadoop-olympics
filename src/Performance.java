/* Anastasia Tsilepi 2022 2015 00179 dit15179
 * Stefanos Mandalas 2022 2017 00107 dit17107 */

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.util.StringTokenizer;

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

public class Performance {
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

    public CustomWritable(IntWritable id, Text name, IntWritable sex) {
      this.id = id;
      this.name = name;
      this.sex = sex;
    }

    public IntWritable getId() { return id; }
    public Text getName() { return name; }
    public IntWritable getSex() { return sex; }

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

    // this is necessary because reducer needs to know how to order keys
    public int compareTo(CustomWritable cw) { return id.compareTo(cw.id); }

    @Override
    public String toString() {
      return id + " " + name + " " + Sex.values()[sex.get()];
    }
  }

  public static class PerformanceMapper
  extends Mapper<Object, Text, CustomWritable, IntWritable> {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
      StringReader stringReader = new StringReader(value.toString());
      BufferedReader csvReader = new BufferedReader(stringReader);
      //csvReader.readLine(); // discard first line (titles)
      String row;
      while ((row = csvReader.readLine()) != null) {
        String[] data = row.split(",");

        String idstr = data[0];
        if (idstr.length() != 0) // id field might be empty
          idstr = idstr.substring(1, idstr.length() - 1); // ex.: "123" -> 123
        int id;
        try {
          id = Integer.parseInt(idstr);
        } catch(NumberFormatException nfe) { id = 0; }

        String name = data[1];
        if (name.length() != 0)
          name = name.substring(1, name.length() - 1);

        String sexstr = data[2];
        if (sexstr.length() != 0)
          sexstr = sexstr.substring(1, sexstr.length() - 1);
        Sex sex = Sex.NA;
        if (sexstr.equals("M"))
          sex = Sex.M;
        else if (sexstr.equals("F"))
          sex = Sex.F;

        String medalstr = data[14];
        if (medalstr.length() != 0)
          medalstr = medalstr.substring(1, medalstr.length() - 1);
        int medal = medalstr.equals("Gold") ? 1 : 0;

        context.write(
          new CustomWritable(id, name, sex),
          new IntWritable(medal)
        );
      }
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
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}