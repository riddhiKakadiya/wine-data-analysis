import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class VarietyWritable implements Writable {

    double avg;
    long count;

    public VarietyWritable() {
        super();
    }

    @Override
    public String toString() {
        return "avg=" + avg + ", count=" + count ;
    }

    public double getAvg() {
        return avg;
    }

    public void setAvg(double avg) {
        this.avg = avg;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public VarietyWritable(double sum, long count) {
        super();
        this.avg = sum;
        this.count = count;
    }
    public void write(DataOutput d) throws IOException {
        d.writeDouble(avg);
        d.writeLong(count);
    }

    public void readFields(DataInput di) throws IOException {
        avg = di.readDouble();
        count = di.readLong();
    }
}
