/**
 * Copyright (C), 2015-2019, 学习
 * FileName: WordCountApp
 * Author:   stg05
 * Date:     2019/5/15 15:01
 * Description: 词频 JOB
 * History:
 */
package com.hadoop.study.mapreduce.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;

/**
 * 〈词频 JOB〉
 *
 * @author stg05
 * @create 2019/5/15
 * @since 1.0.0
 */
public class WordCountApp {

    public static void main(String[] args) throws Exception{

        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");

        System.setProperty("HADOOP_USER_NAME","hadoop");

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://192.168.222.128:9000");

        Job job = Job.getInstance(configuration);
        // 设置JOB对应的主类
        job.setJarByClass(WordCountApp.class);

        // 设置map/reducer 的类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 添加 combiner 操作
        job.setCombinerClass(WordCountReducer.class);

        // 设置 map 的输入输出
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置 reducer 的输入输出
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 作业输入和输出的路径
        FileInputFormat.setInputPaths(job,new Path("/wordCount/input"));
        // 如果输出目录已经存在，则先删除
        FileSystem fs = FileSystem.get(new URI("hdfs://192.168.222.128:9000"),configuration,"hadoop");
        Path outPath = new Path("/wordCount/output");
        if(fs.exists(outPath)){
            fs.delete(outPath,true);
        }
        FileOutputFormat.setOutputPath(job,outPath);

        // 提交 job
        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }

}
 
