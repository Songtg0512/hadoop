/**
 * Copyright (C), 2015-2019, 学习
 * FileName: MyObServer
 * Author:   stg05
 * Date:     2019/6/5 15:00
 * Description: Region级别的ObServer
 * History:
 */
package com.hadoop.study.hbase.coprocessor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * 〈Region级别的ObServer〉
 *
 * @author stg05
 * @create 2019/6/5
 * @since 1.0.0
 */
public class MyObServer extends BaseRegionObserver {

    private Logger logger = LoggerFactory.getLogger(MyObServer.class);

    static Configuration configuration = null;

    static Connection connection = null;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.zookeeper.quorum","hadoop111");
        configuration.set("hbase.master","hadoop111:60000");

        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    @Override
    public void postGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results) {
        logger.info("start method");
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) {

        logger.info("end method");

        byte[] row = put.getRow();

        List<Cell> cells = put.get("cf".getBytes(), "name".getBytes());

        Cell cell = cells.get(0);
        try {
            Table table = connection.getTable(TableName.valueOf("testAPINew"));
            Put newPut = new Put(cell.getValueArray());
            newPut.addColumn("cf".getBytes(),"age".getBytes(),row);
            table.put(newPut);
            table.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }
}
 
