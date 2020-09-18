package com.twq.dao;

import com.twq.hbase.storage.Event;

import java.util.List;

public interface EventHBaseDao {

    public List<Event> getEvents(List<byte[]> rowkeys);

}
