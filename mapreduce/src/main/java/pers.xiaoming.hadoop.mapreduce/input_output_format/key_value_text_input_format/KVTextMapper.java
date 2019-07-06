package pers.xiaoming.hadoop.mapreduce.input_output_format.key_value_text_input_format;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class KVTextMapper extends Mapper<Text, Text, Text, LongWritable> {
    LongWritable v = new LongWritable();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        String[] valuesStr = value.toString().split(" ");
        Set<Long> values = Arrays.stream(valuesStr).map(Long::valueOf).collect(Collectors.toSet());
        long sum = 0;
        for (long x : values) {
            sum += x;
        }

        v.set(sum);
        context.write(key, v);
    }
}
