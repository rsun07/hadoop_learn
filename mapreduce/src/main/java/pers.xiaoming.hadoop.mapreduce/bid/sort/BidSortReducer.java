package pers.xiaoming.hadoop.mapreduce.bid.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pers.xiaoming.hadoop.mapreduce.bid.models.Bid;

import java.io.IOException;

public class BidSortReducer extends Reducer<Bid, Text, Bid, Text> {
    @Override
    protected void reduce(Bid key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
