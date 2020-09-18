package com.twq.dao;

import java.util.List;

public interface EventSolrDao {

    public List<byte[]> getRowkeys(String eventType, String partName);

}
