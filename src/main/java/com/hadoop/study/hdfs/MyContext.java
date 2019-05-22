/**
 * Copyright (C), 2015-2019, 学习
 * FileName: MyContext
 * Author:   stg05
 * Date:     2019/5/14 17:04
 * Description: 自定义上下文-实现Map混村
 * History:
 */
package com.hadoop.study.hdfs;

import java.util.HashMap;
import java.util.Map;

/**
 * 〈自定义上下文-实现Map缓存〉
 *
 * @author stg05
 * @create 2019/5/14
 * @since 1.0.0
 */
public class MyContext {

    private Map<Object,Object> cacheMap = new HashMap<>();

    public Map<Object,Object> getCacheMap(){
        return cacheMap;
    }

    /**
     *  写数据到缓存中
     * @param key 单词
     * @param value 次数
     */
    public void write(Object key, Object value){
        cacheMap.put(key,value);
    }

    /**
     *  从缓存中获取值
     * @param key 单词
     * @return 单词出现的次数
     */
    public Object get(Object key){
        return cacheMap.get(key);
    }



}
 
