package com.twq.dao.impl;

import com.twq.dao.EventSolrDao;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

@Repository
public class EventSolrDaoImpl implements EventSolrDao {
    @Override
    public List<byte[]> getRowkeys(String eventType, String partName) {
        List<byte[]> rowkeys = new ArrayList<>();
        if (StringUtils.isEmpty(eventType) && StringUtils.isEmpty(partName)) {
            return rowkeys;
        }

        CloudSolrClient solrClient = new CloudSolrClient.Builder()
                .withZkHost("master:2181,slave1:2181,slave2:2181")
                .withZkChroot("/solr").build();
        solrClient.connect();

        solrClient.setDefaultCollection("sensor");

        ModifiableSolrParams params = new ModifiableSolrParams();
        params.set("qt", "/select");
        String query = null;
        if (StringUtils.isEmpty(eventType)) {
            query = String.format("+partName:%s", partName);
        } else if (StringUtils.isEmpty(partName)) {
            query = String.format("+eventType:%s", eventType);
        } else {
            query = String.format("+eventType:%s +partName:%s", eventType, partName);
        }
        params.set("q", query);

        QueryResponse response = null;
        try {
            response = solrClient.query(params);
        } catch (SolrServerException e) {
            e.printStackTrace();
            return rowkeys;
        } catch (IOException e) {
            e.printStackTrace();
            return rowkeys;
        }

        SolrDocumentList docs = response.getResults();
        docs.forEach(new Consumer<SolrDocument>() {
            @Override
            public void accept(SolrDocument entries) {
                rowkeys.add((byte[])entries.getFieldValue("rowkey"));
            }
        });
        return rowkeys;
    }
}
