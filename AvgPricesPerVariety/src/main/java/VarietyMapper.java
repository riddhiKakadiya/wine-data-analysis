/* average price of each variety of */

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class VarietyMapper extends Mapper<LongWritable,Text, Text, VarietyWritable> {
    public void map(LongWritable key,Text value, Context ctx) throws IOException, InterruptedException{
        try {
            String[] tokens = value.toString().split("\\t");
            if(!(tokens.length <= 7)){
                if (!tokens[7].equals("variety") || !(tokens[7].equals(""))){
                    Text VarietyKey = new Text(tokens[7]) ;
                    VarietyWritable priceValue = new VarietyWritable(Double.parseDouble(tokens[3]), 1);
                    ctx.write(VarietyKey, priceValue);
        }}}
        catch(NumberFormatException ne) {
            System.out.println("Headers");
        }
    }

}
