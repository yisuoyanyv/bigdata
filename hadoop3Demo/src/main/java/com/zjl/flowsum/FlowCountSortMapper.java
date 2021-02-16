package com.zjl.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * mapper输出的value时自定义的类
 *
 * Mapper<LongWritable,Text,FlowBean,Text> 与FlowCountMapper中次序有调整
 */
public class FlowCountSortMapper extends Mapper<LongWritable,Text,FlowBean,Text> {
    FlowBean bean=new FlowBean();
    Text v=new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1 获取一行
        String line = value.toString();
        // 2 切割字段
        String[] fields = line.split("\t");
        // 3 封装对象
        //取出收集号码
        String phoneNum=fields[0];
        //取出上行流量和下行流量
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);

        bean.set(upFlow,downFlow);
        v.set(phoneNum);

        // 4 写出
        context.write(bean,v);
    }
}
