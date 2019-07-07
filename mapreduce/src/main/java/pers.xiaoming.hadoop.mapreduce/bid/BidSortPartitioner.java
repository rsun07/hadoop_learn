package pers.xiaoming.hadoop.mapreduce.bid;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import pers.xiaoming.hadoop.mapreduce.bid.models.Bid;

public class BidSortPartitioner extends Partitioner<Bid, Text> {
    @Override
    public int getPartition(Bid bid, Text text, int i) {
        int targetId = bid.getTargetId();

        switch(targetId / 100) {
            case 30 : return 0;
            case 31 : return 1;
            case 32 : return 2;
            default : return 0;
        }
    }
}
