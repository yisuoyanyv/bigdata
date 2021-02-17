package com.zjl.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class OrderMapper  extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    private String filename;

    private OrderBean order=new OrderBean();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取切片文件名
        FileSplit fs=(FileSplit)context.getInputSplit();
        filename=fs.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        //对不同数据来源分开处理
        if("order.txt".equals(filename)){
            order.setId(fields[0]);
            order.setPid(fields[1]);
            order.setAmount(Integer.parseInt(fields[2]));
            order.setPname("");
        }else{
            order.setPid(fields[0]);
            order.setPname(fields[1]);
            order.setAmount(0);
            order.setId("");
        }

        context.write(order,NullWritable.get());
    }
}
