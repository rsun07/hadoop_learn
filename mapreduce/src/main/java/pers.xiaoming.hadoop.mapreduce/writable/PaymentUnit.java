package pers.xiaoming.hadoop.mapreduce.writable;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * To describe the payment to worker based on the unit they completed
 * and the price per unit
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class PaymentUnit implements Writable {
    private String workerName;
    private int unitId;
    private double unitPrice;
    private int numOfUnitCompleted;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(workerName);
        dataOutput.writeInt(unitId);
        dataOutput.writeDouble(unitPrice);
        dataOutput.writeInt(numOfUnitCompleted);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.workerName = dataInput.readUTF();
        this.unitId = dataInput.readInt();
        this.unitPrice = dataInput.readDouble();
        this.numOfUnitCompleted = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "PaymentUnit{" +
                "workerName='" + workerName + '\'' +
                ", unitId=" + unitId +
                ", unitPrice=" + unitPrice +
                ", numOfUnitCompleted=" + numOfUnitCompleted +
                '}';
    }
}
