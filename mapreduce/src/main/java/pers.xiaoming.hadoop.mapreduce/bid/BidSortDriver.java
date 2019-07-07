package pers.xiaoming.hadoop.mapreduce.bid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pers.xiaoming.hadoop.mapreduce.bid.models.Bid;

import java.io.IOException;

public class BidSortDriver {
    private static final int DEFAULT_NUM_PARTITION = 1;
    private final String inputPath;
    private final String outputPath;
    private final Job job;

    public BidSortDriver(String inputPath, String outputPath) throws IOException {
        this(inputPath, outputPath, DEFAULT_NUM_PARTITION);
    }

    public BidSortDriver(String inputPath, String outputPath, int numPartition) throws IOException {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.job = setupJob(numPartition);
    }

    private Job setupJob(int numPartition) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(BidSortDriver.class);

        job.setMapperClass(BidSortMapper.class);
        job.setReducerClass(BidSortReducer.class);
        job.setPartitionerClass(BidSortPartitioner.class);
        job.setNumReduceTasks(numPartition);

        job.setMapOutputKeyClass(Bid.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Bid.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public boolean run() throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }
}
