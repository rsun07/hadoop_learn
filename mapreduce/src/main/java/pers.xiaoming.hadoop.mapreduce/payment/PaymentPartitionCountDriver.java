package pers.xiaoming.hadoop.mapreduce.payment;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentResult;
import pers.xiaoming.hadoop.mapreduce.payment.models.PaymentUnit;

import java.io.IOException;

public class PaymentPartitionCountDriver {
    private final String inputPath;
    private final String outputPath;
    private final Job job;

    public PaymentPartitionCountDriver(String inputPath, String outputPath) throws IOException {
        this.inputPath = inputPath;
        this.outputPath = outputPath;

        this.job = setupJob();
    }

    private Job setupJob() throws IOException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(PaymentPartitionCountDriver.class);

        job.setMapperClass(PaymentCountMapper.class);
        job.setReducerClass(PaymentCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PaymentUnit.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PaymentResult.class);

        // set partitioner class
        job.setPartitionerClass(PaymentPartitioner.class);


        // if numReduceTasks > numPartition, result to some empty output file
        // if 1 < numReduceTasks < numPartition, some partition data has no reducerTask to handle, Exception throw
        // numReduceTask == 1, the only one reduce task will handle all partitions and write the result into one single output file
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public boolean run() throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }

}
