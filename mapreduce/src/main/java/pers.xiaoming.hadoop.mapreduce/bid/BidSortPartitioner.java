package pers.xiaoming.hadoop.mapreduce.bid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import pers.xiaoming.hadoop.mapreduce.bid.models.Bid;

public class BidSortPartitioner extends Partitioner<Text, Bid> {
    @Override
    public int getPartition(Text text, Bid bid, int i) {
        return bid.getTargetId() - 1;
    }
}
