import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class Driver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

      try {
          Configuration conf = new Configuration();
          Job job = Job.getInstance(conf, "count");
          job.setJarByClass(Driver.class);

          job.setMapperClass(TasterMapper.class);
          job.setCombinerClass(TasterReducer.class);
          job.setReducerClass(TasterReducer.class);

          job.setInputFormatClass(TextInputFormat.class);
          job.setOutputFormatClass(TextOutputFormat.class);

          job.setMapOutputKeyClass(Text.class);
          job.setMapOutputValueClass(IntWritable.class);

          job.setOutputKeyClass(Text.class);
          job.setOutputValueClass(IntWritable.class);

          job.setNumReduceTasks(1);

          FileInputFormat.addInputPath(job, new Path(args[0]));
          FileOutputFormat.setOutputPath(job, new Path(args[1]));
          System.exit(job.waitForCompletion(true) ? 0 : 1);
      }
      catch (Exception e){e.printStackTrace(System.out);}
    }
}
