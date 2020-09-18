package com.twq.dao.impl;

import com.twq.dao.EventHBaseDao;
import com.twq.hbase.storage.CellEventConvertor;
import com.twq.hbase.storage.Event;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Repository
public class EventHBaseDaoImpl implements EventHBaseDao {
    @Override
    public List<Event> getEvents(List<byte[]> rowkeys) {
        List<Event> events = new ArrayList<>();
        if (rowkeys.isEmpty()) {
            return events;
        }
        List<Get> gets = new ArrayList<>();
        rowkeys.forEach(new Consumer<byte[]>() {
            @Override
            public void accept(byte[] rowkey) {
                gets.add(new Get(rowkey));
            }
        });

        Configuration configuration = HBaseConfiguration.create();

        try (Connection connection = ConnectionFactory.createConnection(configuration);
             Table table = connection.getTable(TableName.valueOf("sensor"))) {

            Result[] results = table.get(gets);
            for (Result result : results) {
                if (result != null && !result.isEmpty()) {
                    while (result.advance()) {
                        Event event = new CellEventConvertor().cellToEvent(result.current(), null);
                        events.add(event);
                    }
                } else {
                    System.out.println("Impossible to find requested cell");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            return events;
        }
        return events;
    }
}
