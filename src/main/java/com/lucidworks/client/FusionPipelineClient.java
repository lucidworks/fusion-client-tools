package com.lucidworks.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.*;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.common.SolrException;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.protocol.HttpClientContext;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.net.ConnectException;
import java.net.MalformedURLException;
import java.net.SocketException;
import java.util.*;

public class FusionPipelineClient {

  private static final Log log = LogFactory.getLog(FusionPipelineClient.class);

  // for basic auth to the pipeline service
  private static final class PreEmptiveBasicAuthenticator implements HttpRequestInterceptor {
    private final UsernamePasswordCredentials credentials;

    public PreEmptiveBasicAuthenticator(String user, String pass) {
      credentials = new UsernamePasswordCredentials(user, pass);
    }

    public void process(HttpRequest request, HttpContext context) throws HttpException, IOException {
      request.addHeader(BasicScheme.authenticate(credentials, "US-ASCII", false));
    }
  }

  // holds a context and a client object
  static class ClientAndContext {
    CloseableHttpClient httpClient;
    HttpClientContext context;
  }

  static Map<String,ClientAndContext> establishSession(List<String> endpoints, String user, String password, String realm) throws Exception {

    ClientAndContext cnc = new ClientAndContext();
    cnc.context = HttpClientContext.create();

    RequestConfig globalConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.BEST_MATCH).build();
    CookieStore cookieStore = new BasicCookieStore();
    cnc.context.setCookieStore(cookieStore);

    Map<String,ClientAndContext> map = new HashMap<String, ClientAndContext>();
    for (String url : endpoints) {

      HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
      httpClientBuilder.setDefaultRequestConfig(globalConfig).setDefaultCookieStore(cookieStore);

      if (user != null && realm == null) {
        httpClientBuilder.addInterceptorFirst(new PreEmptiveBasicAuthenticator(user, password));
      }

      CloseableHttpClient httpClient = httpClientBuilder.build();
      HttpClientUtil.setMaxConnections(httpClient, 500);
      HttpClientUtil.setMaxConnectionsPerHost(httpClient, 100);

      if (realm != null) {
        int at = url.indexOf("/api");
        String proxyUrl = url.substring(0,at);
        String sessionApi = proxyUrl + "/api/session?realmName=" + realm;
        String jsonString = "{\"username\":\"" + user + "\", \"password\":\"" + password + "\"}"; // TODO: ugly!
        HttpPost postRequest = new HttpPost(sessionApi);
        postRequest.setEntity(new StringEntity(jsonString, ContentType.create("application/json", StandardCharsets.UTF_8)));
        HttpResponse response = httpClient.execute(postRequest, cnc.context);
        HttpEntity entity = response.getEntity();
        try {
          int statusCode = response.getStatusLine().getStatusCode();
          if (statusCode != 200 && statusCode != 201 && statusCode != 204) {
            String body = extractResponseBodyText(entity);
            throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
              "POST credentials to Fusion Session API [" + sessionApi + "] failed due to: " +
                response.getStatusLine() + ": " + body);
          }
        } finally {
          if (entity != null)
            EntityUtils.consume(entity);
        }
        log.info("Established secure session with Fusion Session API on " + proxyUrl);
      }

      cnc.httpClient = httpClient;
      map.put(url, cnc);
    }

    return map;
  }

  Map<String,ClientAndContext> httpClients;
  ArrayList<String> endPoints;
  int numEndpoints;
  Random random;
  ObjectMapper jsonObjectMapper;
  String fusionUser = null;
  String fusionPass = null;
  String fusionRealm = null;

  public FusionPipelineClient(String endpointUrl, String fusionUser, String fusionPass, String fusionRealm) throws MalformedURLException {

    this.fusionUser = fusionUser;
    this.fusionPass = fusionPass;

    endPoints = new ArrayList<String>(Arrays.asList(endpointUrl.split(",")));
    try {
      httpClients = establishSession(endPoints, fusionUser, fusionPass, fusionRealm);
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }

    random = new Random();
    numEndpoints = endPoints.size();
    jsonObjectMapper = new ObjectMapper();
    this.fusionRealm = fusionRealm;
  }

  public HttpClient getHttpClient() {
    if (httpClients.isEmpty())
      return null;
    return httpClients.values().iterator().next().httpClient;
  }

  protected String getLbEndpoint(List<String> list) {
    int num = list.size();
    if (num == 0)
      return null;

    return list.get((num > 1) ? random.nextInt(num) : 0);
  }

  protected void closeClients() {
    for (ClientAndContext cnc : httpClients.values()) {
      try {
        cnc.httpClient.close();
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    httpClients.clear();
  }

  public void postBatchToPipeline(List docs) throws Exception {
    int numDocs = docs.size();
    String jsonBody = jsonObjectMapper.writeValueAsString(docs);
    if (numEndpoints > 1) {
      Exception lastExc = null;
      ArrayList<String> mutable = (ArrayList<String>)endPoints.clone();

      // try all the endpoints until success is reached
      for (int e=0; e < numEndpoints; e++) {
        String endpoint = getLbEndpoint(mutable);
        if (endpoint == null) {
          // no more endpoints available ... fail
          if (lastExc != null) {
            throw lastExc;
          } else {
            throw new RuntimeException("No Fusion pipeline endpoints available! Check logs for previous errors.");
          }
        }

        if (log.isDebugEnabled())
          log.debug("POSTing batch of "+numDocs+" input docs to "+endpoint);

        try {
          postJsonToPipeline(endpoint, jsonBody);
          if (lastExc != null) {
            log.info("Re-try request to "+endpoint+" succeeded after seeing a "+lastExc);
          }
          break; // if we get here then the request was accepted
        } catch (SolrException solrExc) {
          if (solrExc.code() == 404) {

            log.warn("Received a 404 from " + endpoint + ", re-trying after waiting 1 sec ...");
            Thread.sleep(1000);
            postJsonToPipeline(endpoint, jsonBody);
            log.info("Re-try succeeded after a 404 from " + endpoint);
            break;

          } else {
            throw solrExc;
          }
        } catch (Exception exc) {
          if (shouldRetry(exc)) {
            log.error("Failed to send updates to '"+endpoint+"' due to communication error (will retry): "+exc);
            // try another endpoint but update the cloned list to avoid re-hitting the one having an error
            mutable.remove(endpoint);
            lastExc = exc;
          } else {
            throw exc;
          }
        }
      }
    } else {
      String endpoint = getLbEndpoint(endPoints);
      if (log.isDebugEnabled())
        log.debug("POSTing batch of "+numDocs+" input docs to "+endpoint);

      try {
        postJsonToPipeline(endpoint, jsonBody);
      } catch (SolrException solrExc) {
        if (solrExc.code() == 404) {
          log.warn("Received a 404 from " + endpoint + ", re-trying after waiting 1 sec ...");
          Thread.sleep(1000);
          postJsonToPipeline(endpoint, jsonBody);
          log.info("Re-try succeeded after a 404 from " + endpoint);
        } else {
          throw solrExc;
        }
      } catch (Exception exc) {
        if (shouldRetry(exc)) {
          log.error("Failed to send updates to '" + endpoint + "' due to communication error (will retry): " + exc);
          // try another endpoint but update the cloned list to avoid re-hitting the one having an error
          postJsonToPipeline(endpoint, jsonBody);
          log.info("Re-try request to "+endpoint+" succeeded after seeing a "+exc);
        } else {
          throw exc;
        }
      }

    }
  }

  private static boolean shouldRetry(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof ConnectException ||
            rootCause instanceof SocketException);
  }

  public void postJsonToPipeline(String endpoint, String jsonBatch) throws Exception {

    ClientAndContext cnc = httpClients.get(endpoint);
    CloseableHttpClient httpClient = cnc.httpClient;
    HttpPost postRequest = new HttpPost(endpoint);
    postRequest.setEntity(new StringEntity(jsonBatch, ContentType.create("application/json", StandardCharsets.UTF_8)));
    HttpResponse response = httpClient.execute(postRequest, cnc.context);
    HttpEntity entity = response.getEntity();
    try {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == 401) {
        // unauth'd - session probably expired? retry to establish
        log.error("unauthorized error when trying to query solr, will re-try to establish session");

        // re-establish the session and re-try the request
        EntityUtils.consume(entity);
        entity = null;
        closeClients();
        httpClients = establishSession(endPoints, fusionUser, fusionPass, fusionRealm);
        cnc = httpClients.get(endpoint);
        httpClient = cnc.httpClient;
        response = httpClient.execute(postRequest, cnc.context);
        entity = response.getEntity();
        statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 || statusCode == 204) {
          log.info("Re-try after session timeout succeeded for: " + endpoint);
        } else {
          raiseFusionServerException(endpoint, entity, statusCode, response);
        }
      } else if (statusCode != 200 && statusCode != 204) {
        raiseFusionServerException(endpoint, entity, statusCode, response);
      }
    } finally {
      if (entity != null)
        EntityUtils.consume(entity);
    }
  }

  protected void raiseFusionServerException(String endpoint, HttpEntity entity, int statusCode, HttpResponse response) {
    String body = extractResponseBodyText(entity);
    throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
      "POST doc to [" + endpoint + "] failed due to: " + response.getStatusLine() + ": " + body);
  }

  static String extractResponseBodyText(HttpEntity entity) {
    StringBuilder body = new StringBuilder();
    if (entity != null) {
      BufferedReader reader = null;
      String line = null;
      try {
        reader = new BufferedReader(new InputStreamReader(entity.getContent()));
        while ((line = reader.readLine()) != null)
          body.append(line);
      } catch (Exception ignore) {
        // squelch it - just trying to compose an error message here
        log.warn("Failed to read response body due to: "+ignore);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (Exception ignore){}
        }
      }
    }
    return body.toString();
  }

  public void shutdown() {
    if (httpClients != null) {
      closeClients();
      httpClients = null;
    }
  }
}
