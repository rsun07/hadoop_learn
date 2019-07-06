package pers.xiaoming.hadoop.mapreduce.payment;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentUnit;

/**
 * Partition based on workship id the worker belongs to
 */
public class PaymentPartitioner extends Partitioner<Text, PaymentUnit> {
    @Override
    public int getPartition(Text text, PaymentUnit paymentUnit, int i) {
        String[] fileds = text.toString().split(",");
        int workshopId = Integer.valueOf(fileds[1]);
        return workshopId - 1;
    }
}
