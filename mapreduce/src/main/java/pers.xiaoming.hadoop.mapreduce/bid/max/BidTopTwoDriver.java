package pers.xiaoming.hadoop.mapreduce.bid.max;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class BidTopTwoDriver {
    private final String inputPath;
    private final String outputPath;
    private final Job job;

    public BidTopTwoDriver(String inputPath, String outputPath) throws IOException {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.job = setupJob();
    }

    private Job setupJob() throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(BidTopTwoDriver.class);

        job.setMapperClass(BidTopTwoMapper.class);
        job.setReducerClass(BidTopTwoReducer.class);

        // set reducer grouping comparator
        job.setGroupingComparatorClass(BidGroupingComparator.class);

        job.setMapOutputKeyClass(BidTopTwo.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(BidTopTwo.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public boolean run() throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }
}
