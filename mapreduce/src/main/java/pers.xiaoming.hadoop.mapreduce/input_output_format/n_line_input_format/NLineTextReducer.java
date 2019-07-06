package pers.xiaoming.hadoop.mapreduce.input_output_format.n_line_input_format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NLineTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    long sum;
    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }

        v.set(sum);
        context.write(key, v);
    }
}
