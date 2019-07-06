package pers.xiaoming.hadoop.mapreduce.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PaymentCountMapper extends Mapper<LongWritable, Text, IntWritable, PaymentUnit> {

    IntWritable k = new IntWritable();
    PaymentUnit v = new PaymentUnit();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split(" ");

        int workId = Integer.parseInt(fields[0]);

        String unitName = fields[1];

        double unitPrice = Double.parseDouble(fields[2]);

        int numOfUnit = Integer.parseInt(fields[3]);

        k.set(workId);
        v.setWorkerId(workId);
        v.setUnitName(unitName);
        v.setNumOfUnitCompleted(numOfUnit);
        v.setUnitPrice(unitPrice);

        context.write(k, v);
    }
}
