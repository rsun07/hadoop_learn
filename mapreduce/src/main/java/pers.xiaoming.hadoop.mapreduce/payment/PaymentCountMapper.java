package pers.xiaoming.hadoop.mapreduce.payment;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentUnit;

import java.io.IOException;

public class PaymentCountMapper extends Mapper<LongWritable, Text, Text, PaymentUnit> {

    Text k = new Text();
    PaymentUnit.PaymentUnitBuilder vBuilder = PaymentUnit.builder();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] fields = line.split(" ");

        String workName = fields[0];

        int unitId = Integer.parseInt(fields[1]);

        double unitPrice = Double.parseDouble(fields[2]);

        int numOfUnit = Integer.parseInt(fields[3]);

        k.set(workName);
        vBuilder.workerName(workName)
                .unitId(unitId)
                .unitPrice(unitPrice)
                .numOfUnitCompleted(numOfUnit);

        context.write(k, vBuilder.build());
    }
}
