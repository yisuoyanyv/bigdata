package com.zjl.reducejoin;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OrderBean implements WritableComparable<OrderBean> {
    private String id;
    private String pid;
    private int amount;
    private String pname;

    @Override
    public String toString() {
        return id+"\t"+pname+"\t"+amount;
    }

    //按照Pid分组，组内按照pname排序，有pname的在前
    @Override
    public int compareTo(OrderBean o) {
        int compare=this.pid.compareTo(o.pid);
        if(compare==0){
            return o.getPname().compareTo(this.getPname());
        }else{
            return compare;
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(pid);
        out.writeInt(amount);
        out.writeUTF(pname);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
         id = in.readUTF();
         pid=in.readUTF();
         amount=in.readInt();
         pname=in.readUTF();

    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }
}
