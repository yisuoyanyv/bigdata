package com.zjl.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HdfsClient {

    @Test
    public void testHdfsClient() throws IOException,InterruptedException{
        //1.  创建HDFS客户端对象，传入uri, configuration,user
        FileSystem fileSystem = FileSystem.get(URI.create("hdfs://hadoop102:9820"), new Configuration(), "zjl");

        //2. 操控集群
        //例如：在集群的/目录下创建testHDFS目录
        fileSystem.mkdirs(new Path("/testHDFS"));

        //3.关闭资源
        fileSystem.close();
    }

    @Test
    public void testCopyFromLocalFile() throws IOException,InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        //设置副本数位2个
        configuration.set("dfs.replication","2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9820"),configuration,"zjl");

        //2 上传文件
        fs.copyFromLocalFile(new Path("e:/test/users.json"),new Path("/users3.json"));
        //3 关闭资源
        fs.close();
    }

    @Test
    public void testCopyToLocalFile() throws IOException,InterruptedException,URISyntaxException{
        //1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9820"),configuration,"zjl");

        //2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否要开启文件校验
        fs.copyToLocalFile(false,new Path("/users.json"),new Path("e:/test/users2.json"),true);

        //3 关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws IOException,InterruptedException,URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs=FileSystem.get(new URI("hdfs://hadoop102:9820"),configuration,"zjl");
        // 2 执行删除
        fs.delete(new Path("/test/"),true);

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testRename() throws IOException,InterruptedException,URISyntaxException{
        // 1 获取文件系统
        Configuration configuration=new Configuration();
        FileSystem fs= FileSystem.get(new URI("hdfs://hadoop102:9820"),configuration,"zjl");
        // 2 修改文件名称
        fs.rename(new Path("/users.json"),new Path("/users2.json"));
        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws IOException,InterruptedException,URISyntaxException{
        // 1 获取文件系统
        Configuration configuration=new Configuration();
        FileSystem fs=FileSystem.get(new URI("hdfs://hadoop102:9820"),configuration,"zjl");
        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
        while (listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            //输出详情
            //文件名称
            System.out.println(status.getPath().getName());
            //长度
            System.out.println(status.getLen());
            //权限
            System.out.println(status.getPermission());
            //分组
            System.out.println(status.getGroup());
            //获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
                // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
                System.out.println("----------------漂亮的分割线------------------");
            }
        }

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testListStatus() throws IOException,InterruptedException,URISyntaxException{
        // 1 获取文件系统
        Configuration configuration=new Configuration();
        FileSystem fs=FileSystem.get(new URI("hdfs://hadoop102:9820"),configuration,"zjl");
        // 2 判断是文件还是文件夹
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : fileStatuses) {
            // 如果是文件
            if(fileStatus.isFile()){
                System.out.println("f:" + fileStatus.getPath().getName());
            }else{
                System.out.println("d:" +  fileStatus.getPath().getName());
            }
        }

        // 3 关闭资源
        fs.close();
    }
}
