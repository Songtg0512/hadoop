/**
 * Copyright (C), 2015-2019, 学习
 * FileName: AccessDriver
 * Author:   stg05
 * Date:     2019/5/16 13:49
 * Description: 流量统计 -- 自定义Driver
 * History:
 */
package com.hadoop.study.mapreduce.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 〈流量统计 -- 自定义Driver〉
 *
 * @author stg05
 * @create 2019/5/16
 * @since 1.0.0
 */
public class AccessDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        /*System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");

        System.setProperty("HADOOP_USER_NAME","hadoop");*/

        Configuration configuration = new Configuration();

        configuration.set("fs.defaultFS","hdfs://hadoop111:9000");

        Job job = Job.getInstance(configuration);

        job.setJarByClass(AccessDriver.class);

        job.setMapperClass(AccessMapper.class);
        job.setReducerClass(AccessReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Access.class);

        // 设置分区
        job.setPartitionerClass(AccessPartitioner.class);
        // 设置reducer个数
        job.setNumReduceTasks(3);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Access.class);

        /*
            本地运行
            FileInputFormat.setInputPaths(job,new Path("access/input"));
            FileOutputFormat.setOutputPath(job,new Path("access/output"));
         */
        // 部署到Yarn
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : 1);
    }

}
 
