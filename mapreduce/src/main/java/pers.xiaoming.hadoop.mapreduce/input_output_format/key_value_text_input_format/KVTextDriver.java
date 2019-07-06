package pers.xiaoming.hadoop.mapreduce.input_output_format.key_value_text_input_format;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import pers.xiaoming.hadoop.mapreduce.helloworld.WordCountDriver;
import pers.xiaoming.hadoop.mapreduce.helloworld.WordCountMapper;
import pers.xiaoming.hadoop.mapreduce.helloworld.WordCountReducer;

import java.io.IOException;

public class KVTextDriver {
    private final String inputPath;
    private final String outputPath;
    private final Job job;

    public KVTextDriver(String inputPath, String outputPath) throws IOException {
        this.inputPath = inputPath;
        this.outputPath = outputPath;

        this.job = setupJob();
    }

    private Job setupJob() throws IOException {
        Configuration configuration = new Configuration();

        // set separator for input key and value
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, ",");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(KVTextDriver.class);

        job.setMapperClass(KVTextMapper.class);
        job.setReducerClass(KVTextReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // set input format class
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        return job;
    }

    public boolean run() throws InterruptedException, IOException, ClassNotFoundException {
        return job.waitForCompletion(true);
    }
}
