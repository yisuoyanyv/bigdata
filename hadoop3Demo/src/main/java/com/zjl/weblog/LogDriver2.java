package com.zjl.weblog;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 解析日志的复杂业务流程
 */
public class LogDriver2 {

    public static void main(String[] args) throws Exception{
        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args=new String[]{"E:\\test\\web.log","E:\\test\\webEtloutPut2"};

        // 1 获取job信息
        Configuration configuration=new Configuration();
        Job job=Job.getInstance(configuration);

        // 2 加载jar包
        job.setJarByClass(LogDriver2.class);

        // 3 关联map
        job.setMapperClass(LogMapper2.class);

        // 4 设置最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //设置reduceTask个数为0
        job.setNumReduceTasks(0);
        // 5 设置输入和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 6 提交
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
