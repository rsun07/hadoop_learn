package pers.xiaoming.hadoop.mapreduce.input_output_format.n_line_input_format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class NLineTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    long sum = 0;
    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        for (LongWritable value : values) {
            sum += value.get();
        }

        v.set(sum);
        context.write(key, v);
    }
}
