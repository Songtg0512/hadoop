/**
 * Copyright (C), 2015-2019, 学习
 * FileName: AccessReducer
 * Author:   stg05
 * Date:     2019/5/16 13:43
 * Description: 流量统计 -- 自定义Reducer
 * History:
 */
package com.hadoop.study.mapreduce.access;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * 〈流量统计 -- 自定义Reducer〉
 *
 * @author stg05
 * @create 2019/5/16
 * @since 1.0.0
 */
public class AccessReducer extends Reducer<Text,Access, NullWritable,Access> {

    @Override
    protected void reduce(Text key, Iterable<Access> values, Context context) throws IOException, InterruptedException {

        long up = 0;
        long down = 0;

        for(Access access : values){
            up += access.getUp();
            down += access.getDown();
        }
        context.write(NullWritable.get(),new Access(key.toString(),up,down));
    }
}
 
