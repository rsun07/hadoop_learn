package pers.xiaoming.hadoop.mapreduce.bid.max;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BidTopTwo implements WritableComparable<BidTopTwo> {
    private int targetId;
    private double price;
    private String bidderName;

    @Override
    public int compareTo(BidTopTwo that) {
        int diff = this.targetId - that.targetId;

        if (diff > 0) {
            return 1;
        } else if (diff < 0) {
            return -1;
        } else {
            double priceDiff = this.price - that.price;
            if (priceDiff > 0) {
                return -1;
            } else if (priceDiff < 0) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(targetId);
        dataOutput.writeDouble(price);
        dataOutput.writeUTF(bidderName);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.targetId = dataInput.readInt();
        this.price = dataInput.readDouble();
        this.bidderName = dataInput.readUTF();
    }
}
