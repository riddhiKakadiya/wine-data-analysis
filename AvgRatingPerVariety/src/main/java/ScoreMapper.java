/* average score of each variety of grapes*/

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
        import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ScoreMapper extends Mapper<LongWritable,Text, Text, ScoreWritable> {
    public void map(LongWritable key,Text value, Context ctx) throws IOException, InterruptedException{
        try {
            String[] tokens = value.toString().split("\\t");
            if(!(tokens.length <= 7)){
                if (!tokens[7].equals("variety") || !(tokens[7].equals(""))){
                    Text VarietyKey = new Text(tokens[7]) ;
                    ScoreWritable priceValue = new ScoreWritable(Double.parseDouble(tokens[2]), 1);
                    ctx.write(VarietyKey, priceValue);
                }}}
        catch(NumberFormatException ne) {
            System.out.println("Headers");
        }
    }

}
