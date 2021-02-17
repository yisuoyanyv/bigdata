package com.zjl.reducejoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class OrderDriver {

    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(OrderDriver.class);

        job.setMapperClass(OrderMapper.class);
        job.setReducerClass(OrderReducer.class);

        job.setGroupingComparatorClass(OrderWritableComparator.class);

        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,new Path("e:\\test\\input\\order.txt"),new Path("e:\\test\\input\\pd.txt"));
        //FileInputFormat.setInputPaths(job,new Path("e:\\test\\input"));
        //如果换成这种文件夹路径的写法，会报错：
        //Exception in thread "main" java.lang.UnsatisfiedLinkError: org.apache.hadoop.io.nativeio.NativeIO$POSIX.stat(Ljava/lang/String;)Lorg/apache/hadoop/io/nativeio/NativeIO$POSIX$Stat;
        FileOutputFormat.setOutputPath(job,new Path("e:\\test\\output"));

        boolean result = job.waitForCompletion(true);
        System.exit(result? 0:1);


    }
}
