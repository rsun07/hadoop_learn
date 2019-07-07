package pers.xiaoming.hadoop.mapreduce.bid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pers.xiaoming.hadoop.mapreduce.bid.models.Bid;

import java.io.IOException;

public class BidSortReducer extends Reducer<Text, Bid, Text, Bid> {
    @Override
    protected void reduce(Text key, Iterable<Bid> values, Context context) throws IOException, InterruptedException {
        for (Bid bid : values) {
            context.write(key, bid);
        }
    }
}
