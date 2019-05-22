/**
 * Copyright (C), 2015-2019, 学习
 * FileName: HDFSWCApp
 * Author:   stg05
 * Date:     2019/5/14 16:47
 * Description: HDFS 词频统计
 * History:
 */
package com.hadoop.study.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * 〈HDFS 词频统计〉
 *
 *  1） 读取HDFS上的文件
 *  2） 业务处理：对文件中的每一行进行处理，按照固定的分隔符
 *  3） 将结果进行缓存
 *  4） 将结果输出到HDFS
 */
public class HDFSWCApp {

    public static void main(String[] args) throws Exception {

        Properties properties = ParamsUtil.getProperties();

        // 1. 读取HDFS文件
        Path input = new Path(properties.getProperty(Constants.INPUT_PATH));
        FileSystem fs = FileSystem.get(new URI(properties.getProperty(Constants.HDFS_ADDRESS)),new Configuration(),"hadoop");
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(input,true);

        // 通过反射创建对象
        Class<?> clazz = Class.forName(properties.getProperty(Constants.MAPPER_CLASS));
        WCMapperImpl mapper = (WCMapperImpl) clazz.newInstance();
        /*
            原版通过new的方式创建对象
            IMyMapper mapper = new WCMapperImpl();
        */
        MyContext context = new MyContext();

        while(iterator.hasNext()){
            LocatedFileStatus status = iterator.next();
            FSDataInputStream in = fs.open(status.getPath());
            BufferedReader reader = new BufferedReader(new InputStreamReader(in));

            String line = "";
            while((line = reader.readLine()) != null){
                // 2.词频统计 (hello,3)
                mapper.map(line,context);
            }
            reader.close();
            in.close();
        }

        // 3. 对结果进行缓存
        Map<Object,Object> contextMap = context.getCacheMap();

        // 4. 结果输出
        Path output = new Path(properties.getProperty(Constants.OUTPUT_PATH));

        // 在output文件夹下，生成wc.txt文件
        FSDataOutputStream out = fs.create(new Path(output,new Path(properties.getProperty(Constants.OUTPUT_FILE))));
        Set<Map.Entry<Object, Object>> entries = contextMap.entrySet();
        for (Map.Entry<Object,Object> entry : entries){
            out.write((entry.getKey().toString() + "\t" + entry.getValue() + "\n").getBytes());
        }
        out.close();
        System.out.println("使用HDFS进行词频统计，程序运行成功！");
    }
}
 
