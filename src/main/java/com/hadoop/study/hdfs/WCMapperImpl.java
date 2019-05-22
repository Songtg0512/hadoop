/**
 * Copyright (C), 2015-2019, 学习
 * FileName: WCMapperImpl
 * Author:   stg05
 * Date:     2019/5/14 17:11
 * Description: Mapper 实现类
 * History:
 */
package com.hadoop.study.hdfs;

/**
 * 〈Mapper 实现类〉
 *
 * @author stg05
 * @create 2019/5/14
 * @since 1.0.0
 */
public class WCMapperImpl implements IMyMapper {

    @Override
    public void map(String line, MyContext context) {
        line = line.toUpperCase();
        String[] words = line.split(" ");
        for(String word : words){
            Object value = context.get(word);
            if(null == value){ // 没有出现过该单词
                context.write(word,1);
            }else{// 取出单词出现的次数 +  1
                context.write(word,Integer.parseInt(value.toString()) + 1);
            }
        }
    }
}
 
