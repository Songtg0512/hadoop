/**
 * Copyright (C), 2015-2019, 学习
 * FileName: MyObServer
 * Author:   stg05
 * Date:     2019/6/5 15:00
 * Description: Region级别的ObServer
 * History:
 */
package com.hadoop.study.hbase.coprocessor;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * 〈Region级别的ObServer〉
 *
 * @author stg05
 * @create 2019/6/5
 * @since 1.0.0
 */
public class MyObServer extends BaseRegionObserver {

    private Logger logger = LoggerFactory.getLogger(MyObServer.class);

    static Configuration conf = null;
    static Connection conn;

    static{
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.property.clientPort","2181");
        conf.set("hbase.zookeeper.quorum","hadoop111");
        conf.set("hbase:master","hadoop111:60000");

        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void preGetOp(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<Cell> results)
            throws IOException {
        super.preGetOp(e, get, results);
        logger.info("00000000000000000000000000000000000000000000000000000000000000$$$$");
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) {
        try {
            byte[] row = put.getRow();

            List<Cell> list = put.get("cf".getBytes(),"name".getBytes());
            Cell cell = list.get(0);

            Table table = conn.getTable(TableName.valueOf("person8"));
            Put put_new  = new Put(cell.getValueArray());
            put_new.addColumn("cf".getBytes(),"age".getBytes(),row);

            table.put(put_new);
            table.close();
        } catch (Exception e2) {
            // TODO: handle exception
        }
    }

}
 
