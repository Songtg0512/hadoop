/**
 * Copyright (C), 2015-2019, 学习
 * FileName: WordCountMapper
 * Author:   stg05
 * Date:     2019/5/15 14:43
 * Description: Mapper - 词频
 * History:
 */
package com.hadoop.study.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 〈Mapper - 词频〉
 *
 * @author stg05
 * @create 2019/5/15
 * @since 1.0.0
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 切分数据
        String[] lines = value.toString().toUpperCase().split("\t");

        for(String line : lines){
            context.write(new Text(line),new IntWritable(1));
        }

    }
}
 
