package pers.xiaoming.hadoop.mapreduce.input_output_format.key_value_text_input_format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class KVTextReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable v = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long sum = 0;

        for (LongWritable value : values) {
            sum += value.get();
        }

        v.set(sum);
        context.write(key, v);
    }
}
