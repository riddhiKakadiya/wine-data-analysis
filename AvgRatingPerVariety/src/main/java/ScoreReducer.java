import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ScoreReducer extends Reducer<Text, ScoreWritable, Text, ScoreWritable> {
    double sum = 0.0;
    long count = 0;
    @Override
    public void reduce(Text key, Iterable<ScoreWritable> values, Context ctx) throws IOException, InterruptedException{
        for(ScoreWritable val: values){
            sum+=val.getAvg()*val.getCount();
            count+=val.getCount();
        }
        ScoreWritable result = new ScoreWritable(sum/count, count);
        ctx.write(key, result);
    }
}
