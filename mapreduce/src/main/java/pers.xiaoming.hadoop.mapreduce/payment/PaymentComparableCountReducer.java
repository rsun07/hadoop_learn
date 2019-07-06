package pers.xiaoming.hadoop.mapreduce.payment;

import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentResult;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentResultComparable;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentUnit;

import java.io.IOException;

public class PaymentComparableCountReducer extends Reducer<Text, PaymentUnit, PaymentResultComparable, NullWritable> {

    PaymentResultComparable k;
    NullWritable nullWritable = NullWritable.get();

    @Override
    protected void reduce(Text key, Iterable<PaymentUnit> values, Context context) throws IOException, InterruptedException {
        double sumPayment = 0.0;
        k = new PaymentResultComparable();

        // get worker name and the workshopId it belongs to
        String[] fields = key.toString().split(",");
        String workerName = fields[0];
        k.setWorkerName(workerName);
        k.setWorkshopId(Integer.valueOf(fields[1]));

        for (PaymentUnit paymentUnit : values) {
            int unitId = paymentUnit.getUnitId();
            double unitPrice = paymentUnit.getUnitPrice();
            int numOfUnit = paymentUnit.getNumOfUnitCompleted();

            k.getCompletedUnits().put(new IntWritable(unitId), new DoubleWritable(unitPrice));

            sumPayment += numOfUnit * unitPrice;
        }

        k.setTotalPayment(Precision.round(sumPayment, 2));

        context.write(k, nullWritable);
    }
}
