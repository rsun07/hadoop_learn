package pers.xiaoming.hadoop.mapreduce.input_output_format.n_line_input_format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class NLineTextMapper extends Mapper<Text, Text, Text, LongWritable> {
    Text k = new Text();
    LongWritable v = new LongWritable(1);

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split(" ");

        k.set(key + "," + Arrays.toString(fields));
        context.write(key, v);
    }
}
