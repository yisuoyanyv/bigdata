package com.zjl.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 使用 FlowCountDriver 的输出作为FlowCountSortDriver的输入。对结果进行排序
 */
public class FlowCountSortDriver {
    public static void main(String[] args) throws IllegalArgumentException, IOException,ClassNotFoundException,InterruptedException {
        //输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args=new String[]{"E:\\test\\phoneOut\\part-r-00000", "e:/test/phoneOutSortPartition"};

        // 1 获取配置信息，或者job对象实例
        Configuration configuration=new Configuration();
        Job job = Job.getInstance(configuration);

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(FlowCountSortDriver.class);

        // 3 指定本业务job要使用的mapper/Reducer业务类
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        // 4 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(Text.class);

        // 5 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 8 加载自定义分区类
        job.setPartitionerClass(ProvincePartitionerBeanKey.class);
        // 设置 Reducetask个数
        job.setNumReduceTasks(5);

        // 6 指定job的输入原始文件所在目录及输出目录
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn区运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0:1);

    }
}
