/**
 * Copyright (C), 2015-2019, 学习
 * FileName: IPTest
 * Author:   stg05
 * Date:     2019/5/17 15:57
 * Description: ip解析类测试
 * History:
 */
package com.hadoop.study.mr.ip;

import com.hadoop.study.mapreduce.project.utils.IPParser;
import org.junit.Test;

/**
 * 〈ip解析类测试〉
 *
 * @author stg05
 * @create 2019/5/17
 * @since 1.0.0
 */
public class IPTest {


    @Test
    public void testIp(){
        IPParser.RegionInfo regionInfo = IPParser.getInstance().analyseIp("111.205.43.241");

        System.out.println(regionInfo.getCity());
        System.out.println(regionInfo.getCountry());
        System.out.println(regionInfo.getProvince());
    }

}
 
