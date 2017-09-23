package com.lucidworks.client;


import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.util.NamedList;

public class FusionQueryResponse extends QueryResponse {

  private NamedList<Object> fusionResponse;

  public FusionQueryResponse(NamedList<Object> res ,SolrClient solrClient) {
    super(res, solrClient);
  }

  @Override
  public void setResponse(NamedList<Object> res) {
    super.setResponse(res);
    for( int i=0; i<res.size(); i++ ) {
      String n = res.getName( i );
      if ("fusion".equals(n)) {
        fusionResponse = (NamedList<Object>) res.getVal(i);
      }
    }
  }

  public NamedList<Object> getFusionResponse() {
    return fusionResponse;
  }
}
