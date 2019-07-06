package pers.xiaoming.hadoop.mapreduce.payment.models;

import org.apache.hadoop.io.WritableComparable;

public class PaymentResultComparable extends PaymentResult implements WritableComparable<PaymentResult> {

    // Sort based on total payment amount
    @Override
    public int compareTo(PaymentResult that) {
        double diff = this.getTotalPayment() - that.getTotalPayment();
        if (diff > 0) {
            return 1;
        } else if (diff == 0) {
            return 0;
        } else {
            return -1;
        }
    }
}
