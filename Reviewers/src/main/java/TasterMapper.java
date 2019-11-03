/**/

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TasterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable count = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {

        try {
            String[] tokens = value.toString().split("\\t");
            if(!(tokens.length <= 5)){
                if (!tokens[5].equals("taster_name") || !(tokens[5].equals(""))){
                    String taster = tokens[5];
                    context.write(new Text(taster), count);
                }
            }

        }
        catch (Exception e){e.printStackTrace(System.out);}
    }
}
