package com.lucidworks.client;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;

import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ResponseHeader {
  public int status;
  public int QTime;
  public Map<String, Object> params;

  @Override
  public String toString() {
    return "ResponseHeader{" +
        "status=" + status +
        ", QTime=" + QTime +
        ", params=" + params +
        '}';
  }
}
