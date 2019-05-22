/**
 * Copyright (C), 2015-2019, 学习
 * FileName: ProvinceStatApp
 * Author:   stg05
 * Date:     2019/5/17 16:40
 * Description: 省份统计
 * History:
 */
package com.hadoop.study.mapreduce.project.v1;

import com.hadoop.study.mapreduce.project.utils.IPParser;
import com.hadoop.study.mapreduce.project.utils.LogParser;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;

/**
 * 〈省份统计〉
 *
 * @author stg05
 * @create 2019/5/17
 * @since 1.0.0
 */
public class ProvinceStatApp {


    public static void main(String[] args) throws Exception {
        System.setProperty("hadoop.home.dir", "D:\\hadoop-2.6.0");

        System.setProperty("HADOOP_USER_NAME", "hadoop");

        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(configuration);
        if (fs.exists(new Path("output/v1/Province"))) {
            fs.delete(new Path("output/v1/Province"), true);
        }

        Job job = Job.getInstance(configuration);

        job.setJarByClass(ProvinceStatApp.class);

        job.setMapperClass(myMapper.class);
        job.setReducerClass(myReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path("input/raw/trackinfo_20130721.data"));
        FileOutputFormat.setOutputPath(job, new Path("output/v1/Province"));

        boolean result = job.waitForCompletion(true);

        System.exit(result ? 0 : -1);
    }

    static class myMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

        private LogParser logParser;

        LongWritable ONE = new LongWritable(1);

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            logParser = new LogParser();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String log = value.toString();

            Map<String, String> map = logParser.parse(log);

            IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp(map.get("ip"));
            if (regionInfo != null) {
                String province = regionInfo.getProvince();
                if (StringUtils.isNotBlank(province)) {
                    context.write(new Text(province), ONE);
                } else {
                    context.write(new Text("-"), ONE);
                }
            } else {
                context.write(new Text("-"), ONE);
            }

        }
    }

    static class myReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
            long count = 0;
            for (LongWritable value : values) {
                count++;
            }
            context.write(key, new LongWritable(count));
        }
    }

}
 
