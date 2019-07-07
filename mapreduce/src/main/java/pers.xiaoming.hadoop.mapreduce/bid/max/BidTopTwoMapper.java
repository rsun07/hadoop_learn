package pers.xiaoming.hadoop.mapreduce.bid.max;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class BidTopTwoMapper extends Mapper<LongWritable, Text, BidTopTwo, NullWritable> {
    BidTopTwo k;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        k = new BidTopTwo();
        String bidderName = fields[0];
        k.setBidderName(bidderName);
        k.setTargetId(Integer.parseInt(fields[1]));

        double price = Precision.round(Double.parseDouble(fields[2]), 2);
        k.setPrice(price);

        context.write(k, NullWritable.get());
    }
}
