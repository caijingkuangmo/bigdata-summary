package com.twq;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * create 'twq:webtable','content','language','link_url'
 */
public class HBaseClientTest {
    public static void main(String[] args) throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "master,slave1,slave2");

        //注意需要设置java的版本为8
        try(Connection connection = ConnectionFactory.createConnection(configuration)) {
            Table table = connection.getTable(TableName.valueOf("twq:webtable"));

            table.put(DataGenerator.getPuts(100000));
        }

    }
}
