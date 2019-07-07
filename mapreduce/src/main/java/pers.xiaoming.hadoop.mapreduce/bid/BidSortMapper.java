package pers.xiaoming.hadoop.mapreduce.bid;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pers.xiaoming.hadoop.mapreduce.bid.models.Bid;

import java.io.IOException;

public class BidSortMapper extends Mapper<LongWritable, Text, Text, Bid> {
    Text k = new Text();
    Bid v;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = key.toString().split("\t");

        String bidderName = fields[0];
        k.set(bidderName);

        v = new Bid();
        v.setTargetId(Integer.parseInt(fields[1]));

        double price = Precision.round(Double.parseDouble(fields[2]), 2);
        v.setPrice(price);

        context.write(k, v);
    }
}
