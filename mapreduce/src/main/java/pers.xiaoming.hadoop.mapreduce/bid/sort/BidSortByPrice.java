package pers.xiaoming.hadoop.mapreduce.bid.sort;

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
public class BidSortByPrice implements WritableComparable<BidSortByPrice> {
    private int targetId;
    private double price;

    @Override
    public int compareTo(BidSortByPrice that) {
        double diff = this.price - that.getPrice();

        if (diff > 0) {
            return -1;
        } else if (diff == 0) {
            return 0;
        } else {
            return 1;
        }
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(targetId);
        dataOutput.writeDouble(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.targetId = dataInput.readInt();
        this.price = dataInput.readDouble();
    }
}
