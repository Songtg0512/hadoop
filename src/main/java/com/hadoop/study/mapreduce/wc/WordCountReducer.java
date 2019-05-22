/**
 * Copyright (C), 2015-2019, 学习
 * FileName: WordCountReducer
 * Author:   stg05
 * Date:     2019/5/15 14:52
 * Description: Reducer - 词频
 * History:
 */
package com.hadoop.study.mapreduce.wc;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.io.Text;
import java.io.IOException;

/**
 * 〈Reducer - 词频〉
 *
 * @author stg05
 * @create 2019/5/15
 * @since 1.0.0
 */
public class WordCountReducer extends Reducer<Text,IntWritable,Text,IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        int sum = 0;

        for(IntWritable value : values){
           sum += value.get();
        }

        context.write(key,new IntWritable(sum));
    }
}
 
