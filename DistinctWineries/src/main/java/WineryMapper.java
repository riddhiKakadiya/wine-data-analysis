import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WineryMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
    private IntWritable count = new IntWritable(1);
    @Override
    protected void map(LongWritable key, Text value,Context context)
            throws IOException, InterruptedException {

        try {
            String[] tokens = value.toString().split("\\t");
            if(!(tokens.length <= 8)){
                if (!tokens[8].equals("taster_name") || !(tokens[8].equals(""))){
                    String winery = tokens[8];
                    context.write(new Text(winery ), count);
                }
            }
        }
        catch (Exception e){e.printStackTrace(System.out);}
    }
}
