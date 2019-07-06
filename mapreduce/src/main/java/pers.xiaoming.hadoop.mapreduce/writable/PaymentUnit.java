package pers.xiaoming.hadoop.mapreduce.writable;

import lombok.AllArgsConstructor;
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
@NoArgsConstructor
@AllArgsConstructor
public class PaymentUnit implements Writable {
    private int workerId;
    private String unitName;
    private double unitPrice;
    private int numOfUnitCompleted;

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(workerId);
        dataOutput.writeUTF(unitName);
        dataOutput.writeDouble(unitPrice);
        dataOutput.writeInt(numOfUnitCompleted);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.workerId = dataInput.readInt();
        this.unitName = dataInput.readUTF();
        this.unitPrice = dataInput.readDouble();
        this.numOfUnitCompleted = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "PaymentUnit{" +
                "workerId=" + workerId +
                ", unitName='" + unitName + '\'' +
                ", unitPrice=" + unitPrice +
                ", numOfUnitCompleted=" + numOfUnitCompleted +
                '}';
    }
}
