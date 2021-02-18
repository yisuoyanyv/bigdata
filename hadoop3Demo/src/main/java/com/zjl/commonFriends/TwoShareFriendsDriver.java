package com.zjl.commonFriends;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TwoShareFriendsDriver {
    public static void main(String[] args) throws Exception{
        //使用第一次的输出作为输入
        args=new String[]{"E:\\test\\commonFriends\\output1\\part-r-00000","E:\\test\\commonFriends\\output2"};
        // 1 获取job对象
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        // 2 指定jar包运行的路径
        job.setJarByClass(TwoShareFriendsDriver.class);

        // 3 指定map和reduce
        job.setMapperClass(TwoShareFriendsMapper.class);
        job.setReducerClass(TwoShareFriendsReducer.class);

        // 4 指定map kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 5 指定最终输出 kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 6 指定job的输入输出目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 提交job相关配置到yarn
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
