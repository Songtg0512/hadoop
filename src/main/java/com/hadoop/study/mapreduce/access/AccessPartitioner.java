/**
 * Copyright (C), 2015-2019, 学习
 * FileName: AccessPartitioner
 * Author:   stg05
 * Date:     2019/5/16 15:22
 * Description: 自定义分区
 * History:
 */
package com.hadoop.study.mapreduce.access;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 〈自定义分区〉
 *
 * @author stg05
 * @create 2019/5/16
 * @since 1.0.0
 */
public class AccessPartitioner extends Partitioner<Text,Access> {

    @Override
    public int getPartition(Text text, Access access, int i) {
        if(text.toString().startsWith("13")){
            return 0;
        }else if(text.toString().startsWith("15")){
            return 1;
        }else{
            return 2;
        }
    }
}
 
