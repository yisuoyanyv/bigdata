package com.zjl.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class DistributedCacheMapper  extends Mapper<LongWritable,Text, Text, NullWritable> {

    //pd表在内存中的缓存
    private Map<String,String>  pMap=new HashMap<>();

    private Text line = new Text();

    //任务开始前将pd数据缓存进PMAP

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //从缓存文件中找到pd.txt
        URI[] cacheFiles = context.getCacheFiles();
        Path path = new Path(cacheFiles[0]);
        //获取文件系统并开流
        FileSystem fileSystem=FileSystem.get(context.getConfiguration());
        FSDataInputStream fsDataInputStream = fileSystem.open(path);

        //通过包装转换为reader
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(fsDataInputStream, "utf-8"));
        //逐行读取，按行处理
        String line;
        while (StringUtils.isNotEmpty(line= bufferedReader.readLine())){
            String[] fields = line.split("\t");
            pMap.put(fields[0],fields[1]);

        }
        //关流
        IOUtils.closeStream(bufferedReader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        String pname = pMap.get(fields[1]);
        //从缓存中取出并替换
        line.set(fields[0]+"\t"+pname+"\t"+fields[2]);
        context.write(line,NullWritable.get());

    }
}
