package com.zjl.wordcount;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 在Windows上向集群提交任务
 */
public class WordcountDriverOnWindowsToYarn {
    public static void main(String[] args) throws IOException,ClassNotFoundException,InterruptedException {

        // 1 获取配置信息以及封装任务
        Configuration configuration=new Configuration();
        //设置HDFS NameNode的地址
        configuration.set("fs.defaultFS","hdfs://hadoop102:9820");
        //指定MapReduce运行在Yarn上
        configuration.set("mapreduce.framework.name","yarn");
        //指定mapreduce可以在远程集群运行
        configuration.set("mapreduce.app-submission.cross-platform","true");
        //指定 Yarn resourcemanager的位置
        configuration.set("yarn.resourcemanager.hostname","hadoop103");
        Job job=Job.getInstance(configuration);

        // 2 设置 jar 加载路径
//        job.setJarByClass(WordcountDriverOnWindowsToYarn.class);
        job.setJar("F:\\workspace\\bigdata\\hadoop3Demo\\target\\hadoop3Demo-1.0-SNAPSHOT.jar");

        //如果不设置InputFormat,它默认用的是 TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        //虚拟存储切片最大值设置4m
        CombineTextInputFormat.setMaxInputSplitSize(job,4194304);

        // 3 设置map和reduce类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        // 4 设置map 输出
        job.setMapOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 5 设置最终输出kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 6 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 提交
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : 1);
    }
}
