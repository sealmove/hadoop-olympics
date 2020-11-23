/* Anastasia Tsilepi 2022 2015 00179 dit15179
 * Stefanos Mandalas 2022 2017 00107 dit17107 */

import java.io.IOException;
import java.io.BufferedReader;
import java.io.StringReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Performance {
  public static class TokenizerMapper
  extends Mapper<Object, Text, IntWritable, IntWritable> {
    public void map(Object key, Text value, Context context)
    throws IOException, InterruptedException {
      StringReader stringReader = new StringReader(value.toString());
      BufferedReader csvReader = new BufferedReader(stringReader);
      String row;
      while ((row = csvReader.readLine()) != null) {
        String[] data = row.split(",");

        String id = data[0];
        if (id.length() != 0) // id field might be empty
          id = id.substring(1, data[0].length() - 1); // ex.: "123" -> 123
        int idInt;
        try {
          idInt = Integer.parseInt(id);
        } catch(NumberFormatException nfe) { idInt = 0; }
        IntWritable idw = new IntWritable(idInt);

        String medal = data[14];
        if (medal.length() != 0)
          medal = data[14].substring(1, data[14].length() - 1);
        IntWritable medalw = new IntWritable(medal.equals("Gold") ? 1 : 0);

        context.write(idw, medalw);
      }
    }
  }
  
  public static class IntSumReducer
  extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
    public void reduce(IntWritable key, Iterable<IntWritable> values,
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
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(IntWritable.class);
    job.setJarByClass(Performance.class);
    for (int i = 0; i < otherArgs.length - 1; ++i)
      FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
    FileOutputFormat.setOutputPath(job,
      new Path(otherArgs[otherArgs.length - 1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}