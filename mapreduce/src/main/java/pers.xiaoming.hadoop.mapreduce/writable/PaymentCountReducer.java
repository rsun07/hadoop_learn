package pers.xiaoming.hadoop.mapreduce.writable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PaymentCountReducer extends Reducer<Text, PaymentUnit, Text, PaymentResult> {

    PaymentResult v = new PaymentResult();

    @Override
    protected void reduce(Text key, Iterable<PaymentUnit> values, Context context) throws IOException, InterruptedException {
        double sumPayment = 0.0;

        v.setWorkerName(key.toString());

        for (PaymentUnit paymentUnit : values) {
            int unitId = paymentUnit.getUnitId();
            double unitPrice = paymentUnit.getUnitPrice();
            int numOfUnit = paymentUnit.getNumOfUnitCompleted();

            v.getCompletedUnits().put(new IntWritable(unitId), new DoubleWritable(unitPrice));

            sumPayment += numOfUnit * unitPrice;
        }

        v.setTotalPayment(sumPayment);

        context.write(key, v);
    }
}
