import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class VarietyReducer extends Reducer<Text, VarietyWritable, Text, VarietyWritable> {
    double sum = 0.0;
    long count = 0;
    @Override
    public void reduce(Text key, Iterable<VarietyWritable> values, Context ctx) throws IOException, InterruptedException{
        for(VarietyWritable val: values){
            sum+=val.getAvg()*val.getCount();
            count+=val.getCount();
        }
        VarietyWritable result = new VarietyWritable(sum/count, count);
        ctx.write(key, result);
    }
}
