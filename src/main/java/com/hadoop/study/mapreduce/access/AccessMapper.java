/**
 * Copyright (C), 2015-2019, 学习
 * FileName: AccessMapper
 * Author:   stg05
 * Date:     2019/5/16 13:38
 * Description: 流量统计 -- 自定义Mapper
 * History:
 */
package com.hadoop.study.mapreduce.access;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 〈流量统计 -- 自定义Mapper〉
 *
 * @author stg05
 * @create 2019/5/16
 * @since 1.0.0
 */
public class AccessMapper extends Mapper<LongWritable, Text,Text,Access> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] lines = value.toString().split("\t");

        // 取出手机号
        String phone = lines[0];
        // 取出上行流量
        long up = Long.parseLong(lines[lines.length-3]);
        // 取出下行流量
        long down = Long.parseLong(lines[lines.length-2]);

        context.write(new Text(phone),new Access(phone,up,down));
    }
}
 
