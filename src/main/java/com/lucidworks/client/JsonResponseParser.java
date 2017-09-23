package com.lucidworks.client;

import org.apache.solr.common.SolrException;
import org.codehaus.jackson.map.ObjectMapper;
import org.noggit.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;

/**
 * ResponseParser for JsonMaps.  Used for Get and Renew DelegationToken responses.
 */
public class JsonResponseParser {
  private Logger log = LoggerFactory.getLogger(JsonResponseParser.class);

  private ObjectMapper objectMapper;

  public JsonResponseParser(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  public SearchResponse processResponse(InputStream body, String encoding) {
    try {
      return objectMapper.readValue(body, SearchResponse.class);
    } catch (IOException | JSONParser.ParseException e) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR,
          "parsing error", e);
    }
  }

}