package com.twq.service;

import com.twq.hbase.storage.Event;

import java.util.List;

public interface OmneoService {

    public List<Event> getEvents(String eventType, String partName);

}
