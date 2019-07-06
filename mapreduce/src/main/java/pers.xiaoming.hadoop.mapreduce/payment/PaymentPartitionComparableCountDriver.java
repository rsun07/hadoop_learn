package pers.xiaoming.hadoop.mapreduce.payment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentResult;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentResultComparable;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentUnit;

import java.io.IOException;

public class PaymentPartitionComparableCountDriver {
    private static final int DEFAULT_NUM_PARTITION = 1;
    private final String inputPath;
    private final String outputPath;
    private final Job job;

    public PaymentPartitionComparableCountDriver(String inputPath, String outputPath) throws IOException {
        this(inputPath, outputPath, DEFAULT_NUM_PARTITION);
    }

    public PaymentPartitionComparableCountDriver(String inputPath, String outputPath, int numPartition) throws IOException {
        this.inputPath = inputPath;
        this.outputPath = outputPath;
        this.job = setupJob(numPartition);
    }

    private Job setupJob(int numPartition) throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(PaymentPartitionComparableCountDriver.class);

        job.setMapperClass(PaymentCountMapper.class);
        job.setReducerClass(PaymentComparableCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PaymentUnit.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PaymentResultComparable.class);

        // set partitioner class
        job.setPartitionerClass(PaymentPartitioner.class);


        // if numReduceTasks > numPartition, result to some empty output file
        // if 1 < numReduceTasks < numPartition, some partition data has no reducerTask to handle, Exception throw
        // numReduceTask == 1, the only one reduce task will handle all partitions and write the result into one single output file
        job.setNumReduceTasks(numPartition);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public boolean run() throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }

}
