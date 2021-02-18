package com.zjl.top;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TopNDriver {

    public static void main(String[] args) throws Exception{
        args=new String[]{"E:\\test\\topN\\top10input.txt","E:\\test\\topN\\out"};

        // 1 获取配置信息，或者job对象实例
        Configuration conf=new Configuration();
        Job job=Job.getInstance(conf);

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(TopNDriver.class);

        // 3 指定本业务job要使用的mapper/reducer业务类
        job.setMapperClass(TopNMapper.class);
        job.setReducerClass(TopNReducer.class);

        // 4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6 指定job的输入原始文件所在位置
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);
    }
}
