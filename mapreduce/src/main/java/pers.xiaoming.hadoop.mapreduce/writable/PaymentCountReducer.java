package pers.xiaoming.hadoop.mapreduce.writable;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class PaymentCountReducer extends Reducer<IntWritable, PaymentUnit, IntWritable, PaymentResult> {

    IntWritable k = new IntWritable();
    PaymentResult v = new PaymentResult();

    @Override
    protected void reduce(IntWritable key, Iterable<PaymentUnit> values, Context context) throws IOException, InterruptedException {
        double sumPayment = 0.0;

        k.set(key.get());
        v.setWorkerId(key.get());

        for (PaymentUnit paymentUnit : values) {
            String unitName = paymentUnit.getUnitName();
            double unitPrice = paymentUnit.getUnitPrice();
            int numOfUnit = paymentUnit.getNumOfUnitCompleted();

            v.getCompletedUnits().put(new Text(unitName), new DoubleWritable(unitPrice));

            sumPayment += numOfUnit * unitPrice;
        }

        v.setTotalPayment(sumPayment);

        context.write(k, v);
    }
}
