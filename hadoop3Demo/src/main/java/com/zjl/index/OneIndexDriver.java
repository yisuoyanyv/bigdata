package com.zjl.index;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class OneIndexDriver {

    public static void main(String[] args) throws Exception{
        //输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args=new String[]{"E:\\test\\index","E:\\test\\index\\output"};

        Configuration conf=new Configuration();

        Job job=Job.getInstance(conf);
        job.setJarByClass(OneIndexDriver.class);

        job.setMapperClass(OneIndexMapper.class);
        job.setReducerClass(OneIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

//        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileInputFormat.setInputPaths(job,"E:\\test\\index\\a.txt,E:\\test\\index\\b.txt,E:\\test\\index\\c.txt");
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
