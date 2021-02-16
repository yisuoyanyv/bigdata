package com.zjl.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 1．需求
 * 将统计结果按照手机归属地不同省份输出到不同文件中（分区）
 * 样例数据：
 * 1	13736230513	192.196.100.1	www.atguigu.com	2481	24681	200
 * 2	13846544121	192.196.100.2			264	0	200
 * 期望输出数据
 * 	手机号136、137、138、139开头都分别放到一个独立的4个文件中，其他开头的放到一个文件中。
 */
public class ProvincePartitioner extends Partitioner<Text,FlowBean> {
    @Override
    public int getPartition(Text key, FlowBean value, int numPartitions) {
        // 1 获取电话号码的前三位
        String preNum = key.toString().substring(0, 3);

        int partition=4;
        // 2 判断是哪个省
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
