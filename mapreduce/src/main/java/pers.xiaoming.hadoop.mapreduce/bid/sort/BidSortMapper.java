package pers.xiaoming.hadoop.mapreduce.bid.sort;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BidSortMapper extends Mapper<LongWritable, Text, BidSortByPrice, Text> {
    Text v = new Text();
    BidSortByPrice k;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        String bidderName = fields[0];
        v.set(bidderName);

        k = new BidSortByPrice();
        k.setTargetId(Integer.parseInt(fields[1]));

        double price = Precision.round(Double.parseDouble(fields[2]), 2);
        k.setPrice(price);

        context.write(k, v);
    }
}
