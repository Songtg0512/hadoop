/**
 * Copyright (C), 2015-2019, 学习
 * FileName: PVStatApp
 * Author:   stg05
 * Date:     2019/5/17 14:47
 * Description: mapReducer版统计 PV
 * History:
 */
package com.hadoop.study.mapreduce.project.v1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * 〈mapReducer版统计 PV〉
 *
 * @author stg05
 * @create 2019/5/17
 * @since 1.0.0
 */
public class PVStatApp {

    public static void main(String[] args) throws Exception{

        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");

        System.setProperty("HADOOP_USER_NAME","hadoop");

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);
        if(fs.exists(new Path("output/v1/PV"))){
            fs.delete(new Path("output/v1/PV"),true);
        }

        Job job = Job.getInstance(configuration);

        job.setJarByClass(PVStatApp.class);

        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job,new Path("input/raw/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job,new Path("output/v1/PV"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }

    static class myMapper extends Mapper<LongWritable, Text,Text,LongWritable>{

        Text KEY = new Text("key");
        LongWritable COUNT = new LongWritable(1);

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            context.write(KEY,COUNT);
        }
    }

    static class myReducer extends Reducer<Text,LongWritable, NullWritable,LongWritable>{
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            long count = 0;

            for(LongWritable value : values){
                count++;
            }
            context.write(NullWritable.get(),new LongWritable(count));
        }
    }

}
 
