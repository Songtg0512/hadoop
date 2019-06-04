/**
 * Copyright (C), 2015-2019, 学习
 * FileName: HBaseUtil
 * Author:   stg05
 * Date:     2019/6/4 14:41
 * Description: HBase 通用工具类
 * History:
 */
package com.hadoop.study.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * 〈HBase 通用工具类〉
 *
 * @author stg05
 * @create 2019/6/4
 * @since 1.0.0
 */
public class HBaseUtil {

    public static Configuration configuration;
    public static Connection connection;

    static {
        // init configuration
        configuration = HBaseConfiguration.create();
        //init zookeeper 2181
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        //init zookeeper host hadoop111
        configuration.set("hbase.zookeeper.quorum","hadoop111");
        //init hbase master config
        configuration.set("hbase.master","hadoop111:60000");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *  创建表
     * @param tableName 表明
     * @param familys 列族
     */
    public static void createTable(String tableName,String...familys) throws IOException {

        Admin admin = connection.getAdmin();

        HTableDescriptor table = new HTableDescriptor(TableName.valueOf(tableName));

        for (String family : familys) {
            HColumnDescriptor column = new HColumnDescriptor(family);
            table.addFamily(column);
        }

        if(admin.tableExists(TableName.valueOf(tableName))){
            System.err.println(tableName + "already exists");
        }else{
            admin.createTable(table);
            System.out.println(tableName + "created succeed");
            admin.close();
        }
    }

    public static void main(String[] args) throws IOException {
        String tableName = "testAPI";
        String[] familys = new String[]{"cf","cf1","cf2"};
        HBaseUtil.createTable(tableName,familys);
    }
}
 
