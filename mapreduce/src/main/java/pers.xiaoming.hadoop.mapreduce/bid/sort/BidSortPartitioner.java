package pers.xiaoming.hadoop.mapreduce.bid.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class BidSortPartitioner extends Partitioner<BidSortByPrice, Text> {
    @Override
    public int getPartition(BidSortByPrice bid, Text text, int i) {
        int targetId = bid.getTargetId();

        switch(targetId / 100) {
            case 30 : return 0;
            case 31 : return 1;
            case 32 : return 2;
            default : return 0;
        }
    }
}
