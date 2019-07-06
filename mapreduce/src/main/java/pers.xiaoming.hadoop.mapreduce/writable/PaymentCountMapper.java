package pers.xiaoming.hadoop.mapreduce.writable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PaymentCountMapper extends Mapper<LongWritable, Text, IntWritable, PaymentUnit> {

    IntWritable k = new IntWritable();
    PaymentUnit.PaymentUnitBuilder vBuilder = new PaymentUnit.PaymentUnitBuilder();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split(" ");

        int workId = Integer.getInteger(fields[0]);

        String unitName = fields[1];

        double unitPrice = Double.parseDouble(fields[2]);

        int numOfUnit = Integer.parseInt(fields[3]);

        k.set(workId);
        vBuilder.workerId(workId)
                .unitName(unitName)
                .unitPrice(unitPrice)
                .numOfUnitCompleted(numOfUnit);

        context.write(k, vBuilder.build());
    }
}
