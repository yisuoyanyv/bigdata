package com.zjl.reducejoin;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderWritableComparator extends WritableComparator {

    public OrderWritableComparator(){
        super(OrderBean.class,true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        OrderBean aOrderBean=(OrderBean) a;
        OrderBean bOrderBean=(OrderBean) b;
        //分组只需要按照pid进行分组即可。
        return aOrderBean.getPid().compareTo(bOrderBean.getPid());
    }
}
