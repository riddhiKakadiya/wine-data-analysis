import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TitleMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private IntWritable count = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {

        try {
            String[] tokens = value.toString().split("\\t");
            if(!(tokens.length <= 6)){
                if (!tokens[6].equals("taster_name") || !(tokens[6].equals(""))){
                    String title = tokens[6];
                    context.write(new Text(title), count);
                }
            }
        }
        catch (Exception e){e.printStackTrace(System.out);}
    }
}
