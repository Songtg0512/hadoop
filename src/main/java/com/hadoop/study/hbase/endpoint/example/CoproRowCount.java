/**
 * Copyright (C), 2015-2019, 学习
 * FileName: CoproRowCount
 * Author:   stg05
 * Date:     2019/6/10 16:06
 * Description: 1
 * History:
 */
package com.hadoop.study.hbase.endpoint.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.generated.ExampleProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;

import java.io.IOException;
import java.util.Map;

/**
 * 〈1〉
 *
 * @author stg05
 * @create 2019/6/10
 * @since 1.0.0
 */
public class CoproRowCount {

    private static Configuration configuration = null;
    private static Connection connection;
    private static Table table = null;

    static {
        configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.property.clientPort","2181");
        configuration.set("hbase.zookeeper.quorum","hadoop111");
        configuration.set("hbase:master","hadoop111:60000");
        try {
            ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {

        // 创建 request 客户端
        final ExampleProtos.CountRequest countRequest = ExampleProtos.CountRequest.getDefaultInstance();

        try {
            table = connection.getTable(TableName.valueOf("person11"));
            try {
                Map<byte[],Long> map = table.coprocessorService(ExampleProtos.RowCountService.class, null, null, new Batch.Call<ExampleProtos.RowCountService, Long>() {

                    @Override
                    public Long call(ExampleProtos.RowCountService countService) throws IOException {
                        //Rpc Controller
                        ServerRpcController controller = new ServerRpcController();

                        BlockingRpcCallback<ExampleProtos.CountResponse> callback = new BlockingRpcCallback<>();

                        countService.getRowCount(controller,countRequest,callback);

                        // result Response
                        ExampleProtos.CountResponse response = callback.get();

                        if(controller.failedOnException()){
                            throw controller.getFailedOn();
                        }
                        return (response != null && response.hasCount()) ? response.getCount() : 0;
                    }
                });

                int sum = 0;
                int count = 0;
                for(long l : map.values()){
                    sum += l;
                    count++;
                }
            } catch (Throwable throwable) {
                throwable.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
