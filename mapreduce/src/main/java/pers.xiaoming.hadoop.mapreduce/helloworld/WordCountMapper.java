package pers.xiaoming.hadoop.mapreduce.helloworld;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

// Why input format is <LongWritable, Text>?
// Because it uses default TextInputFormat (default impl of FileInputFormat)
// the 'LongWritable' is the offset of each line's first char in the whole document
// the 'Text' is the text of each line
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        String[] words = line.split(" ");

        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}
