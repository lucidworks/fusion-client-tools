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
          log.info("Established secure session with Fusion Session API on " + proxyUrl);
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

    log.info("Established sessions with "+map.size()+" of "+endpoints.size()+" Fusion endpoints.");

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

    try {
      httpClients = establishSession(Arrays.asList(endpointUrl.split(",")), fusionUser, fusionPass, fusionRealm);
    } catch (Exception exc) {
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(exc);
      }
    }
    // only use the endpoints that we were able to establish sessions with
    endPoints = new ArrayList<String>();
    endPoints.addAll(httpClients.keySet());

    random = new Random();
    numEndpoints = endPoints.size();
    jsonObjectMapper = new ObjectMapper();
    this.fusionRealm = fusionRealm;
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

    ArrayList<String> mutable = null;
    synchronized (this) {
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
            log.error("No more endpoints available to retry failed request! raising last seen error: "+lastExc);
            throw lastExc;
          } else {
            throw new RuntimeException("No Fusion pipeline endpoints available! Check logs for previous errors.");
          }
        }

        if (log.isDebugEnabled())
          log.debug("POSTing batch of "+numDocs+" input docs to "+endpoint);

        Exception retryAfterException = postJsonToPipelineWithRetry(e, endpoint, jsonBody, mutable, lastExc);
        if (retryAfterException == null)
          break; // request succeeded ...

        lastExc = retryAfterException;
      }
    } else {
      String endpoint = getLbEndpoint(mutable);
      if (log.isDebugEnabled())
        log.debug("POSTing batch of "+numDocs+" input docs to "+endpoint);
      postJsonToPipelineWithRetry(0, endpoint, jsonBody, mutable, null);
    }
  }

  protected Exception postJsonToPipelineWithRetry(int e, String endpoint, String jsonBody, ArrayList<String> mutable, Exception lastExc) throws Exception {
    Exception retryAfterException = null;
    try {
      postJsonToPipeline(endpoint, jsonBody);
      if (lastExc != null) {
        log.info("Re-try request to "+endpoint+" succeeded after seeing a "+lastExc);
      }
    } catch (Exception exc) {
      // if it was a communication exception or we have more endpoints to try
      if (shouldRetry(exc)) {
        log.error("Failed to send updates to '"+endpoint+"' due to communication error: "+exc);
        // if there is another endpoint to try ...
        if (e+1 < numEndpoints) {
          // try another endpoint but update the cloned list to avoid re-hitting the one having an error
          log.info("Will re-try failed request on next endpoint in the list.");
          mutable.remove(endpoint);
          retryAfterException = exc;
        } else {
          // no other endpoints to try ... brief wait and then retry
          log.info("No more endpoints available to try ... will retry to send to "+endpoint+" after waiting 1 sec");
          try {
            Thread.sleep(1000);
          } catch (InterruptedException ignore) {
            Thread.interrupted();
          }

          postJsonToPipeline(endpoint, jsonBody);
          log.info("Re-try request to " + endpoint + " succeeded after seeing a " + exc +" on the previous attempt");
        }
      } else {
        if (e+1 < numEndpoints) {
          log.error("Failed to send updates to '"+endpoint+"' due to "+exc+"; will retry another Fusion endpoint.");
          // try another endpoint but update the cloned list to avoid re-hitting the one having an error
          mutable.remove(endpoint);
          retryAfterException = exc;
        } else {
          log.error("Request to "+endpoint+" failed due to "+exc+", no more endpoints to try!");
          throw exc;
        }
      }
    }

    return retryAfterException;
  }

  private static boolean shouldRetry(Exception exc) {
    Throwable rootCause = SolrException.getRootCause(exc);
    return (rootCause instanceof ConnectException ||
            rootCause instanceof SocketException);
  }

  public void postJsonToPipeline(String endpoint, String jsonBatch) throws Exception {

    ClientAndContext cnc = null;
    synchronized (this) {
      cnc = httpClients.get(endpoint);
    }

    if (cnc == null) {
      // no context for endpoint ... re-establish
      log.warn("No existing Http client available for "+endpoint+"! Re-establishing session ...");
      Map<String,ClientAndContext> map =
        establishSession(Arrays.asList(endpoint), fusionUser, fusionPass, fusionRealm);
      cnc = map.get(endpoint);

      synchronized (this) {
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
        log.error("unauthorized error when trying to query solr, will re-try to establish session");

        // re-establish the session and re-try the request
        EntityUtils.consume(entity);
        entity = null;

        synchronized(this) {
          closeClients();
          httpClients = establishSession(endPoints, fusionUser, fusionPass, fusionRealm);
          endPoints = new ArrayList<String>();
          endPoints.addAll(httpClients.keySet());
          cnc = httpClients.get(endpoint);
        }

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

  public synchronized void shutdown() {
    if (httpClients != null) {
      closeClients();
      httpClients = null;
    }
  }
}
