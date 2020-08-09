package com.twq.hbase.rowkey.salt;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SaltingGetter {
    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(config);
             Table table = connection.getTable(TableName.valueOf("test_salt"))) {
            KeySalter keySalter = new KeySalter();
            List<String> allKeys = keySalter.getAllRowKeys("boo0001");
            List<Get> gets = new ArrayList<>();

            for (String key : allKeys) {
                Get get = new Get(Bytes.toBytes(key));
                gets.add(get);
            }

            Result[] results = table.get(gets);

            for (Result result : results) {
                if (result != null) {
                    //do something
                }
            }
        }
    }

}
