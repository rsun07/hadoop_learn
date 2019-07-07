package pers.xiaoming.hadoop.mapreduce.bid.max;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BidTopTwoReducer extends Reducer<BidTopTwo, NullWritable, BidTopTwo, NullWritable> {
    @Override
    protected void reduce(BidTopTwo key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (NullWritable value : values) {
            context.write(key, NullWritable.get());
            if (++count == 2) {
                break;
            }
        }
    }
}
