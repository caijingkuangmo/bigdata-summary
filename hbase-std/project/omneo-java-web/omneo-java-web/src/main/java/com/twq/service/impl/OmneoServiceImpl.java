package com.twq.service.impl;

import com.twq.dao.EventHBaseDao;
import com.twq.dao.EventSolrDao;
import com.twq.hbase.storage.Event;
import com.twq.service.OmneoService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class OmneoServiceImpl implements OmneoService {
    @Autowired
    private EventSolrDao eventSolrDao;
    @Autowired
    private EventHBaseDao eventHBaseDao;

    @Override
    public List<Event> getEvents(String eventType, String partName) {
        //1: 根据eventType和partName从solr中获取符合条件的rowkey
        List<byte[]> rowkeys = eventSolrDao.getRowkeys(eventType, partName);

        //2: 根据上面的rowkey从hbase中查询出记录，并且转换成Event
        List<Event> events = eventHBaseDao.getEvents(rowkeys);

        return events;
    }
}
