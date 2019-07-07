package pers.xiaoming.hadoop.mapreduce.bid.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class BidSortReducer extends Reducer<BidSortByPrice, Text, BidSortByPrice, Text> {
    @Override
    protected void reduce(BidSortByPrice key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {
            context.write(key, value);
        }
    }
}
