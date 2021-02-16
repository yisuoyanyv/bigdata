package com.zjl.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * FlowBean 作为key的自定义分区类，与ProvincePartitioner 基本相同
 */
public class ProvincePartitionerBeanKey extends Partitioner<FlowBean, Text> {
    @Override
    public int getPartition(FlowBean key, Text value, int i) {
        // 1 获取手机号码前三位
        String preNum = value.toString().substring(0, 3);

        int partition=4;

        // 2 根据手机号归属地设置分区
        if("136".equals(preNum)){
            partition=0;
        }else if ("137".equals(preNum)){
            partition=1;
        }else if ("138".equals(preNum)){
            partition=2;
        }else if ("139".equals(preNum)){
            partition=3;
        }
        return partition;
    }
}