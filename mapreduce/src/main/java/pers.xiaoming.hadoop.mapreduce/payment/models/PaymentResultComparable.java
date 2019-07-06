package pers.xiaoming.hadoop.mapreduce.payment.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@AllArgsConstructor
public class PaymentResultComparable implements WritableComparable<PaymentResultComparable> {
    private String workerName;
    private int workshopId;
    private MapWritable completedUnits;
    private double totalPayment;

    public PaymentResultComparable() {
        super();
        this.completedUnits = new MapWritable();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(workerName);
        dataOutput.writeInt(workshopId);
        completedUnits.write(dataOutput);
        dataOutput.writeDouble(totalPayment);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        workerName = dataInput.readUTF();
        workshopId = dataInput.readInt();
        completedUnits.readFields(dataInput);
        totalPayment = dataInput.readDouble();
    }

    // Sort based on total payment amount
    @Override
    public int compareTo(PaymentResultComparable that) {
        double diff = totalPayment - that.getTotalPayment();
        if (diff > 0) {
            return 1;
        } else if (diff == 0) {
            return 0;
        } else {
            return -1;
        }
    }
}
