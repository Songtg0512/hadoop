/**
 * Copyright (C), 2015-2019, 学习
 * FileName: HDFSApp
 * Author:   stg05
 * Date:     2019/5/14 11:12
 * Description: Java API 操作HDFS
 * History:
 */
package com.hadoop.study;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.net.URI;

/**
 * 〈Java API 操作HDFS〉
 *
 * @author stg05
 * @create 2019/5/14
 * @since 1.0.0
 */
public class HDFSApp {

    private final String HDFS_PATH = "hdfs://hadoop111:9000";
    Configuration configuration = null;
    FileSystem fileSystem = null;

    @Before
    public void setUp() throws Exception {
        configuration = new Configuration();
        /**
         * 第一个参数：获取HDFS路径
         * 第二个参数：Configuration类
         * 第三个参数：访问hdfs的用户名
         */
        fileSystem = FileSystem.get(new URI(HDFS_PATH), configuration, "hadoop");
        System.err.println("----- start -----");
    }

    /**
     * 创建HDFS文件件
     */
    @Test
    public void mkdir() throws Exception{
        fileSystem.mkdirs(new Path("/hdfs/api"));
    }

    /**
     * 读取HDFS文件内容
     */
    @Test
    public void text() throws Exception{
        FSDataInputStream open = fileSystem.open(new Path("/hdfs/api/hadoop配置文件.txt"));
        IOUtils.copyBytes(open,System.out,1024);
    }

    /**
     * 创建文件
     */
    @Test
    public void create(){
        FSDataOutputStream stream = null;
        try {
            stream = fileSystem.create(new Path("/hdfs/api/a.txt"));
            stream.writeUTF("Hello ok!");
            stream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                stream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 文件重命名
     */
    @Test
    public void rename() throws Exception{
        boolean result = fileSystem.rename(new Path("/hdfs/api/a.txt"), new Path("/hdfs/api/b.txt"));
        System.out.println(result);
    }

    /**
     * 拷贝本地文件到HDFS
     */
    @Test
    public void copyFormLocalFile() throws Exception{
        fileSystem.copyFromLocalFile(new Path("D:\\eee.txt"),new Path("/hdfs/api"));
    }

    /**
     * 拷贝大文件到HDFS：带进度显示
     */
    @Test
    public void copyFormLocalFileBig() throws Exception{
        InputStream in = new BufferedInputStream(new FileInputStream(new File("D:\\gradle-4.7-rc-2.zip")));
        // Progressable 上传期间显示输出
        FSDataOutputStream out = fileSystem.create(new Path("/hdfs/api/jdk.tgz"), new Progressable() {
            @Override
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in,out,4096);
    }

    /**
     * 拷贝HDFS文件到本地：下载
     */
    @Test
    public void copyToLocal() throws Exception{
        fileSystem.copyToLocalFile(new Path("/hdfs/api/b.txt"),new Path("D:\\c.txt"));
    }

    /**
     * 列出路径下全部文件
     */
    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/hdfs/api"));
        for (FileStatus file : fileStatuses){
            String type = file.isDir() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long len = file.getLen();
            String path = file.getPath().toString();
            System.out.println(type + "\t" + permission + "\t" + replication + "\t"
                                + len + "\t" + path
            );
        }
    }

    /**
     *  hdfs dfs -ls -R /hdfs/api    ： -R 表示递归
     *  Recursive ： 递归
     */
    @Test
    public void listFilesRecursive() throws Exception{
        RemoteIterator<LocatedFileStatus> files = fileSystem.listFiles(new Path("/hdfs/api"), true);
        while(files.hasNext()){
            LocatedFileStatus file = files.next();
            String type = file.isDir() ? "文件夹" : "文件";
            String permission = file.getPermission().toString();
            short replication = file.getReplication();
            long len = file.getLen();
            String path = file.getPath().toString();
            System.out.println(type + "\t" + permission + "\t" + replication + "\t"
                    + len + "\t" + path
            );
        }
    }

    /**
     * 查看文件块信息
     */
    @Test
    public void getFileBlockLocations() throws Exception{
        FileStatus fileStatus = fileSystem.getFileStatus(new Path("/hdfs/api/hadoop配置文件.txt"));

        BlockLocation[] blocks = fileSystem.getFileBlockLocations(fileStatus, 0, fileStatus.getLen());

        for(BlockLocation block : blocks){
            for (String name : block.getNames()){
                System.out.println(name + " : " + block.getOffset() + " : " + block.getLength());
            }
        }
    }

    /**
     * 删除文件
     */
    @Test
    public void delete() throws Exception{
        // 第2个参数：true递归
        boolean result = fileSystem.delete(new Path("/hdfs/api/b.txt"),false);
        System.out.println(result);
    }

    @After
    public void testDown(){
        configuration = null;
        fileSystem = null;
        System.err.println("----- end -----");
    }
}
 
