package com.zjl.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Combiner  局部汇总
 */
public class WordcountCombiner extends Reducer<Text, IntWritable,Text,IntWritable> {
    IntWritable v=new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // 1 汇总
        int sum=0;
        for (IntWritable value : values) {
            sum+=value.get();
        }
        v.set(sum);
        // 2 写出
        context.write(key,v);
    }
}
