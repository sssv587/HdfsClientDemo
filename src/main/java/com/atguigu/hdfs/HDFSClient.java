package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yuhang.sun
 * @date 2020/8/30 - 16:21
 */
public class HDFSClient {
    public HDFSClient() throws InterruptedException, IOException, URISyntaxException {
    }

    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        Configuration conf = new Configuration();

//        conf.set("fs.defaultFS","hdfs://hadoop102:9000");
//        //1.获取hdfs客户端对象
//        FileSystem fs = FileSystem.get(conf);
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.在hdfs上创建路径
        fs.mkdirs(new Path("/0529/dashen/banzhang"));

        //3.关闭资源
        fs.close();

        System.out.println("over");
    }

    //1.文件上传
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        //1.获取fs对象
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.执行上传API
        fs.copyFromLocalFile(new Path("E:/testfile/banzhang.txt"), new Path("/0529/dashen/xiaohua"));

        //3.关闭资源
        fs.close();
    }

    //2.文件下载
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {

        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.执行下载操作
//        fs.copyToLocalFile(new Path("/0529/dashen/banhua"),new Path("E:/testfile/banhua.txt"));
        fs.copyToLocalFile(false, new Path("/0529/dashen/banhua"), new Path("E:/testfile/xiaohua.txt"), true);

        //3.关闭资源
        fs.close();
    }

    //3.文件删除
    @Test
    public void testDelete() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.文件删除
        fs.delete(new Path("/0529/dashen/banzhang"), true);

        //3.关闭资源
        fs.close();
    }

    //4.文件名修改
    @Test
    public void testRename() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.执行更名
        fs.rename(new Path("/0529/dashen/banhua"), new Path("/0529/dashen/bancao"));

        //3.关闭资源
        fs.close();
    }

    //5.文件详情查看
    @Test
    public void testListFiles() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.查看文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);

        while (listFiles.hasNext()) {
            LocatedFileStatus filestatus = listFiles.next();

            //查看文件名、权限、长度、块信息
            System.out.println(filestatus.getPath().getName()); //文件名称
            System.out.println(filestatus.getPermission()); //文件权限
            System.out.println(filestatus.getLen()); //文件长度

            BlockLocation[] blockLocations = filestatus.getBlockLocations();

            for (BlockLocation blockLocation : blockLocations) {
                String[] hosts = blockLocation.getHosts();

                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("--------分割线----------");
        }
        //3.关闭资源
        fs.close();
    }

    //6.判断是文件还是文件夹
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.判断操作
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {
            if (fileStatus.isFile()) {
                //文件
                System.out.println("f：" + fileStatus.getPath().getName());
            } else {
                //文件夹
                System.out.println("d：" + fileStatus.getPath().getName());
            }
        }

        //3.关闭资源
        fs.close();
    }
}
