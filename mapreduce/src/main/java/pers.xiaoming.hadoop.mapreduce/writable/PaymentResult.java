package pers.xiaoming.hadoop.mapreduce.writable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentResult implements Writable {
    private int workerId;
    private MapWritable completedUnits;
    private double totalPayment;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(workerId);
        completedUnits.write(dataOutput);
        dataOutput.writeDouble(totalPayment);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        workerId = dataInput.readInt();
        completedUnits.readFields(dataInput);
        totalPayment = dataInput.readDouble();
    }
}
