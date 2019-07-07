package pers.xiaoming.hadoop.mapreduce.bid.max;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class BidGroupingComparator extends WritableComparator {

    protected BidGroupingComparator() {
        super(BidTopTwo.class, true);
    }

    // Grouping Comparator is compare and group before data go into reducer
    // Here we pretend all target id same BidTopTwo as same key
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        BidTopTwo bidA = (BidTopTwo) a;
        BidTopTwo bidB = (BidTopTwo) b;
        return bidA.getTargetId() - bidB.getTargetId();
    }
}
