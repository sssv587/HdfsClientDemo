package com.atguigu.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author yuhang.sun
 * @date 2020/8/30 - 22:41
 */
public class HDFSIO {

    //把本地e盘上的banhua.txt文件上传到HDFS根目录
    @Test
    public void putFileToHDFS() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.获取输入流
        FileInputStream fis = new FileInputStream(new File("E:/testfile/banzhang.txt"));

        //3.获取输出流
        FSDataOutputStream fos = fs.create(new Path("/banzhang.txt"));

        //4.流的对接
        IOUtils.copyBytes(fis, fos, conf);

        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //2.从HDFS上下载banhua.txt文件到本地e盘上
    @Test
    public void getFileFromHDFS() throws URISyntaxException, IOException, InterruptedException {

        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/0529/dashen/bancao"));

        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E:/testfile/sss.txt"));

        //4.流的对接
        IOUtils.copyBytes(fis, fos, conf);

        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //下载第一块
    @Test
    public void readFileSeek1() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        //3.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E:/testfile/adoop-2.7.2.tar.gz.part1"));

        //4.流的对接(只拷贝128M)
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        //5.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    //下载第二块
    @Test
    public void readFileSeek2() throws URISyntaxException, IOException, InterruptedException {
        //1.获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://hadoop102:9000"), conf, "root");

        //2.获取输入流
        FSDataInputStream fis = fs.open(new Path("/hadoop-2.7.2.tar.gz"));

        //3.获取指定读取的起点
        fis.seek(1024 * 1024 * 128);

        //4.获取输出流
        FileOutputStream fos = new FileOutputStream(new File("E:/testfile/adoop-2.7.2.tar.gz.part2"));

        //5.流的对接
        IOUtils.copyBytes(fis, fos, conf);

        //6.关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }
}
