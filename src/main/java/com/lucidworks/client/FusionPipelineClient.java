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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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

    Exception lastError = null;
    Map<String,ClientAndContext> map = new HashMap<String, ClientAndContext>();
    for (String url : endpoints) {

      try {
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
          String proxyUrl = url.substring(0, at);
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
          log.info("Established secure session with Fusion Session API on " + proxyUrl+" for user "+user+" in realm "+realm);
        }

        cnc.httpClient = httpClient;
        map.put(url, cnc);
      } catch (Exception exc) {
        // just log this ... so long as there is at least one good endpoint we can use it
        lastError = exc;
        log.error("Failed to establish session with Fusion at "+url+" due to: "+exc);
      }
    }

    if (map.isEmpty()) {
      if (lastError != null) {
        throw lastError;
      } else {
        throw new Exception("Failed to establish session with Fusion endpoint(s): "+endpoints);
      }
    }

    log.info("Established sessions with "+map.size()+" of "+endpoints.size()+
      " Fusion endpoints for user "+user+" in realm "+realm);

    return map;
  }

  Map<String,ClientAndContext> httpClients;
  long sessionTimeoutAt = -1;
  ArrayList<String> endPoints;
  int numEndpoints;
  Random random;
  ObjectMapper jsonObjectMapper;
  String fusionUser = null;
  String fusionPass = null;
  String fusionRealm = null;
  AtomicInteger requestCounter = null;

  public FusionPipelineClient(String endpointUrl, String fusionUser, String fusionPass, String fusionRealm) throws MalformedURLException {

    this.fusionUser = fusionUser;
    this.fusionPass = fusionPass;
    this.fusionRealm = fusionRealm;

    this.endPoints = new ArrayList<String>();
    this.endPoints.addAll(Arrays.asList(endpointUrl.split(",")));
    
    try {
      resetSession();
    } catch (Exception exc) {
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(exc);
      }
    }

    random = new Random();
    numEndpoints = endPoints.size();
    jsonObjectMapper = new ObjectMapper();

    this.requestCounter = new AtomicInteger(0);
  }

  protected synchronized void resetSession() throws Exception {
    closeClients();
    httpClients = establishSession(endPoints, fusionUser, fusionPass, fusionRealm);
    endPoints = new ArrayList<String>();
    endPoints.addAll(httpClients.keySet());
    // session timeout in 10 minutes (10 * 60 seconds) ...
    // so we'll do 9 minutes and 59 seconds in case clocks aren't in-sync
    sessionTimeoutAt = System.nanoTime() + TimeUnit.NANOSECONDS.convert(599, TimeUnit.SECONDS);
  }

  public synchronized HttpClient getHttpClient() {
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

  protected synchronized void closeClients() {

    if (httpClients == null || httpClients.isEmpty())
      return;

    for (ClientAndContext cnc : httpClients.values()) {
      try {
        cnc.httpClient.close();
      } catch (IOException e) {
        log.warn("Failed to close httpClient object due to: "+e);
      }
    }
    httpClients.clear();
  }

  public void postBatchToPipeline(List docs) throws Exception {
    int numDocs = docs.size();
    String jsonBody = jsonObjectMapper.writeValueAsString(docs);

    int requestId = requestCounter.incrementAndGet();
    ArrayList<String> mutable = null;

    synchronized (this) {
      if (System.nanoTime() > sessionTimeoutAt) {
        log.info("Fusion session is likely expired (or soon will be), " +
          "pre-emptively re-setting all sessions before processing request "+requestId);
        resetSession();
      }

      mutable = (ArrayList<String>)endPoints.clone();
    }

    if (numEndpoints > 1) {
      Exception lastExc = null;

      // try all the endpoints until success is reached
      for (int e=0; e < numEndpoints; e++) {
        String endpoint = getLbEndpoint(mutable);
        if (endpoint == null) {
          // no more endpoints available ... fail
          if (lastExc != null) {
            log.error("No more endpoints available to retry failed request ("+requestId+")! raising last seen error: "+lastExc);
            throw lastExc;
          } else {
            throw new RuntimeException("No Fusion pipeline endpoints available to process request "+
              requestId+"! Check logs for previous errors.");
          }
        }

        if (log.isDebugEnabled())
          log.debug("POSTing batch of "+numDocs+" input docs to "+endpoint+" as request "+requestId);

        Exception retryAfterException =
          postJsonToPipelineWithRetry(e, endpoint, jsonBody, mutable, lastExc, requestId);
        if (retryAfterException == null) {
          lastExc = null;
          break; // request succeeded ...
        }

        lastExc = retryAfterException; // try next endpoint (if available) after seeing an exception
      }

      if (lastExc != null) {
        // request failed and we exhausted the list of endpoints to try ...
        log.error("Failing request " + requestId + " due to: " + lastExc);
        throw lastExc;
      }

    } else {
      String endpoint = getLbEndpoint(mutable);
      if (log.isDebugEnabled())
        log.debug("POSTing batch of "+numDocs+" input docs to "+endpoint+" as request "+requestId);

      Exception exc = postJsonToPipelineWithRetry(0, endpoint, jsonBody, mutable, null, requestId);
      if (exc != null)
        throw exc;
    }
  }

  protected Exception postJsonToPipelineWithRetry(int e, String endpoint, String jsonBody,
        ArrayList<String> mutable, Exception lastExc, int requestId)
    throws Exception
  {
    Exception retryAfterException = null;

    try {

      postJsonToPipeline(endpoint, jsonBody, requestId);
      if (lastExc != null)
        log.info("Re-try request "+requestId+" to "+endpoint+" succeeded after seeing a "+lastExc.getMessage());

    } catch (Exception exc) {
      log.error("Failed to send request "+requestId+" to '"+endpoint+"' due to: "+exc);
      // if there is another endpoint to try ...
      if (e+1 < numEndpoints) {
        // try another endpoint but update the cloned list to avoid re-hitting the one having an error
        log.info("Will re-try failed request "+requestId+" on next endpoint in the list");
        mutable.remove(endpoint);
        retryAfterException = exc;
      } else {
        // no other endpoints to try ... brief wait and then retry
        log.info("No more endpoints available to try ... will retry to send request "+
          requestId+" to "+endpoint+" after waiting 1 sec");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignore) {
          Thread.interrupted();
        }
        // note we want the exception to propagate from here up the stack since we re-tried and it didn't work
        postJsonToPipeline(endpoint, jsonBody, requestId);
        log.info("Re-try request " + requestId + " to " +
          endpoint + " succeeded after seeing a " + exc + " on the previous attempt");
        retryAfterException = null; // return success condition
      }
    }

    return retryAfterException;
  }

  private static boolean shouldRetry(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof ConnectException ||
            rootCause instanceof SocketException);
  }

  public void postJsonToPipeline(String endpoint, String jsonBatch, int requestId) throws Exception {

    ClientAndContext cnc = null;

    synchronized (this) {
      cnc = httpClients.get(endpoint);
      if (cnc == null) {
        // no context for endpoint ... re-establish
        log.warn("No existing Http client available for "+endpoint+
          "! Re-establishing session before re-trying request "+requestId+" ...");
        Map<String,ClientAndContext> map =
          establishSession(Arrays.asList(endpoint), fusionUser, fusionPass, fusionRealm);
        cnc = map.get(endpoint);

        if (cnc == null)
          throw new IllegalStateException("Failed to re-connect to "+endpoint+
            " after session loss when processing request "+requestId);

        httpClients.put(endpoint, cnc);
      }
    }

    CloseableHttpClient httpClient = cnc.httpClient;
    HttpPost postRequest = new HttpPost(endpoint);
    postRequest.setEntity(new StringEntity(jsonBatch, ContentType.create("application/json", StandardCharsets.UTF_8)));
    HttpResponse response = httpClient.execute(postRequest, cnc.context);
    HttpEntity entity = response.getEntity();
    try {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == 401) {
        // unauth'd - session probably expired? retry to establish
        log.error("Unauthorized error (401) when trying to send request "+requestId+
          " to Fusion, will re-try to establish session");

        // re-establish the session and re-try the request
        try {
          EntityUtils.consume(entity);
        } catch (Exception ignore) {
          log.warn("Failed to consume entity due to: "+ignore);
        } finally {
          entity = null;
        }

        synchronized(this) {
          resetSession();
          cnc = httpClients.get(endpoint);
          if (cnc == null)
            throw new IllegalStateException("After re-establishing session when processing request "+
              requestId+", endpoint "+endpoint+" is no longer active! Try another endpoint.");
        }

        log.info("Going to re-try request "+requestId+" after session re-established with "+endpoint);
        httpClient = cnc.httpClient;
        response = httpClient.execute(postRequest, cnc.context);
        entity = response.getEntity();
        statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 || statusCode == 204) {
          log.info("Re-try request "+requestId+" after session timeout succeeded for: " + endpoint);
        } else {
          raiseFusionServerException(endpoint, entity, statusCode, response, requestId);
        }
      } else if (statusCode != 200 && statusCode != 204) {
        raiseFusionServerException(endpoint, entity, statusCode, response, requestId);
      }
    } finally {
      if (entity != null)
        EntityUtils.consume(entity);
    }
  }

  protected void raiseFusionServerException(String endpoint, HttpEntity entity, int statusCode, HttpResponse response, int requestId) {
    String body = extractResponseBodyText(entity);
    throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
      "POST request "+requestId+" to [" + endpoint + "] failed due to: ("+statusCode+")" + response.getStatusLine() + ": " + body);
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

  public synchronized void shutdown() {
    if (httpClients != null) {
      closeClients();
      httpClients = null;
    }
  }
}
