package pers.xiaoming.hadoop.mapreduce.input_output_format.n_line_input_format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class NLineTextMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(value, v);
    }
}
