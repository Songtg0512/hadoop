package com.hadoop.study.hdfs;

/**
 *  自定义Mapper
 */
public interface IMyMapper {

    /**
     *  读取每一行数据，写到上下文缓存
     * @param line 行数据
     * @param context 上下文缓存
     */
    void map(String line, MyContext context);

}
