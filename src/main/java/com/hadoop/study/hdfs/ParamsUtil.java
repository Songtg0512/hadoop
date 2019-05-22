/**
 * Copyright (C), 2015-2019, 学习
 * FileName: ParamsUtil
 * Author:   stg05
 * Date:     2019/5/14 17:34
 * Description: 读取配置文件工具类
 * History:
 */
package com.hadoop.study.hdfs;

import java.io.IOException;
import java.util.Properties;

/**
 * 〈读取配置文件工具类〉
 *
 * @author stg05
 * @create 2019/5/14
 * @since 1.0.0
 */
public class ParamsUtil {

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(ParamsUtil.class.getClassLoader().getResourceAsStream("wordCount.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties(){
        return properties;
    }

    public static void main(String[] args) {
        System.out.println(properties.getProperty("INPUT_PATH"));
    }

}
 
