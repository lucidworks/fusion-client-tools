package com.lucidworks.client;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpException;
import org.apache.http.HttpRequest;
import org.apache.http.HttpRequestInterceptor;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CookieStore;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.CookieSpecs;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.cookie.Cookie;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.cookie.BasicClientCookie;
import org.apache.http.protocol.HttpContext;
import org.apache.http.util.EntityUtils;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.solr.client.solrj.impl.XMLResponseParser;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class FusionPipelineClient {

  private static final Log log = LogFactory.getLog(FusionPipelineClient.class);

  public static final String PIPELINE_DOC_TYPE = "application/vnd.lucidworks-document";

  public static final String LWWW_JAAS_FILE = "lww.jaas.file";
  public static final String LWWW_JAAS_APPNAME = "lww.jaas.appname";

  public static String extractFusionHosts(String endpointList) {
    StringBuilder hostAndPortList = new StringBuilder();
    String[] endpoints = endpointList.split(",");
    for (String ep : endpoints) {
      ep = ep.trim();
      if (ep.isEmpty()) continue;

      try {
        URL fusionUrl = new URL(ep);
        if (hostAndPortList.length() > 0) {
          hostAndPortList.append(",");
        }
        int port = fusionUrl.getPort();
        hostAndPortList.append(fusionUrl.getProtocol()).append("://").append(fusionUrl.getHost());
        hostAndPortList.append(":").append(port);
      } catch (MalformedURLException exc) {
        throw new RuntimeException("Cannot parse Fusion endpoint '"+ep+"' into a URL due to: "+exc);
      }
    }
    return hostAndPortList.toString();
  }

  public static String extractPath(String endpointList) {
    String[] endpoints = endpointList.split(",");
    String ep = endpoints[0];
    ep = ep.trim();
    if (ep.isEmpty()) {
      throw new IllegalArgumentException("No Fusion path information available in "+endpointList);
    }

    String path;
    try {
      URL fusionUrl = new URL(ep);
      path = fusionUrl.getPath();
      String queryString = fusionUrl.getQuery();
      if (queryString != null && !path.contains(queryString)) {
        path += "?"+queryString;
      }
    } catch (MalformedURLException exc) {
      throw new RuntimeException("Cannot parse Fusion endpoint '"+ep+"' into a URL due to: "+exc);
    }
    return path;
  }

  public static void setSecurityConfig(String jassFile) {
    if (jassFile == null)
      return;

    log.info("Using kerberized Solr.");
    System.setProperty("sun.security.krb5.debug", "true");
    System.setProperty("java.security.auth.login.config", jassFile);
    final String appname = System.getProperty(LWWW_JAAS_APPNAME, "Client");
    System.setProperty("solr.kerberos.jaas.appname", appname);
    HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
  }

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
  static class FusionSession {
    String id;
    long sessionEstablishedAt = -1;
    Meter docsSentMeter = null;
    HttpSolrClient solrClient = null;

    public String toString() {

      StringBuilder sb = new StringBuilder();
      sb.append(id);
      if (sessionEstablishedAt > 0) {
        sb.append(": ").append(TimeUnit.SECONDS.convert(sessionEstablishedAt, TimeUnit.NANOSECONDS));
      }
      if (docsSentMeter != null) {
        sb.append(", docsSent: ").append(docsSentMeter.getCount());
      }
      return sb.toString();
    }
  }

  List<String> originalHostAndPortList;
  RequestConfig globalConfig;
  CookieStore cookieStore;
  CloseableHttpClient httpClient;

  Map<String, FusionSession> sessions;
  Random random;
  ObjectMapper jsonObjectMapper;
  String fusionUser = null;
  String fusionPass = null;
  String fusionRealm = null;
  AtomicInteger requestCounter = null;
  Map<String, Meter> metersByHost = new HashMap<>();
  boolean isKerberos = false;

  MetricRegistry metrics = null;

  static long maxNanosOfInactivity = TimeUnit.NANOSECONDS.convert(599, TimeUnit.SECONDS);

  public FusionPipelineClient(String fusionHostAndPortList) throws MalformedURLException {
    this(fusionHostAndPortList, null, null, null);
  }

  public FusionPipelineClient(String fusionHostAndPortList, String fusionUser, String fusionPass, String fusionRealm) throws MalformedURLException {

    this.fusionUser = fusionUser;
    this.fusionPass = fusionPass;
    this.fusionRealm = fusionRealm;

    String lwwJaasFile = System.getProperty(LWWW_JAAS_FILE);
    if (lwwJaasFile != null && !lwwJaasFile.isEmpty()) {
      setSecurityConfig(lwwJaasFile);
      httpClient = HttpClientUtil.createClient(null);
      HttpClientUtil.setMaxConnections(httpClient, 1000);
      HttpClientUtil.setMaxConnectionsPerHost(httpClient, 1000);
      isKerberos = true;
    } else {
      globalConfig = RequestConfig.custom().setCookieSpec(CookieSpecs.BEST_MATCH).build();
      cookieStore = new BasicCookieStore();

      // build the HttpClient to be used for all requests
      HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
      httpClientBuilder.setDefaultRequestConfig(globalConfig).setDefaultCookieStore(cookieStore);
      httpClientBuilder.setMaxConnPerRoute(1000);
      httpClientBuilder.setMaxConnTotal(1000);

      if (fusionUser != null && fusionRealm == null)
        httpClientBuilder.addInterceptorFirst(new PreEmptiveBasicAuthenticator(fusionUser, fusionPass));

      httpClient = httpClientBuilder.build();
    }

    originalHostAndPortList = Arrays.asList(fusionHostAndPortList.split(","));
    try {
      sessions = establishSessions(originalHostAndPortList, fusionUser, fusionPass, fusionRealm);
    } catch (Exception exc) {
      if (exc instanceof RuntimeException) {
        throw (RuntimeException) exc;
      } else {
        throw new RuntimeException(exc);
      }
    }

    random = new Random();
    jsonObjectMapper = new ObjectMapper();

    requestCounter = new AtomicInteger(0);
  }

  public String getFusionUser() { return fusionUser; }
  public String getFusionRealm() { return fusionRealm; }

  public void setMetricsRegistry(MetricRegistry metrics) {
    this.metrics = metrics;
  }

  protected Meter getMeterByHost(String meterName, String host) {

    if (metrics == null)
      return null;

    String key = meterName + " (" + host + ")";
    Meter meter = metersByHost.get(key);
    if (meter == null) {
      meter = metrics.meter(meterName + "-" + host);
      metersByHost.put(key, meter);
    }
    return meter;
  }

  protected Map<String, FusionSession> establishSessions(List<String> hostAndPortList, String user, String password, String realm) throws Exception {

    Exception lastError = null;
    Map<String, FusionSession> map = new HashMap<>();
    for (String url : hostAndPortList) {
      String sessionKey = getSessionKey(url);
      if (!map.containsKey(sessionKey)) {
        try {
          FusionSession session = establishSession(sessionKey, user, password, realm);
          map.put(session.id, session);
        } catch (Exception exc) {
          // just log this ... so long as there is at least one good endpoint we can use it
          lastError = exc;
          log.warn("Failed to establish session with Fusion at " + sessionKey + " due to: " + exc);
        }
      }
    }

    if (map.isEmpty()) {
      if (lastError != null) {
        throw lastError;
      } else {
        throw new Exception("Failed to establish session with Fusion host(s): " + hostAndPortList);
      }
    }

    log.info("Established sessions with " + map.size() + " of " + hostAndPortList.size() +
            " Fusion hosts for user " + user + " in realm " + realm);

    return map;
  }

  protected FusionSession establishSession(String sessionKey, String user, String password, String realm) throws Exception {

    if (!sessionKey.startsWith("https://") && !sessionKey.startsWith("http://")) {
      sessionKey = "http://" + sessionKey;
    }

    FusionSession fusionSession = new FusionSession();

    if (!isKerberos && realm != null) {

      String sessionApi = sessionKey + "/api/session?realmName=" + realm;
      String jsonString = "{\"username\":\"" + user + "\", \"password\":\"" + password + "\"}";
      URL sessionApiUrl = new URL(sessionApi);
      String sessionHost = sessionApiUrl.getHost();
      try {
        clearCookieForHost(sessionHost);
      } catch (Exception exc) {
        log.warn("Failed to clear session cookie for " + sessionHost + " due to: " + exc);
      }

      HttpPost postRequest = new HttpPost(sessionApiUrl.toURI());
      postRequest.setEntity(new StringEntity(jsonString, ContentType.create("application/json", StandardCharsets.UTF_8)));

      HttpClientContext context = HttpClientContext.create();
      context.setCookieStore(cookieStore);

      CloseableHttpResponse response = null;
      HttpEntity entity = null;
      try {
        response = httpClient.execute(postRequest, context);
        entity = response.getEntity();

        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode != 200 && statusCode != 201 && statusCode != 204) {
          String body = extractResponseBodyText(entity);
          throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
                  "POST credentials to Fusion Session API [" + sessionApi + "] failed due to: " +
                          response.getStatusLine() + ": " + body);
        } else if (statusCode == 401) {
          // retry in case this is an expired error
          String body = extractResponseBodyText(entity);
          if (body != null && body.indexOf("session-idle-timeout") != -1) {
            EntityUtils.consumeQuietly(entity); // have to consume the previous entity before re-trying the request
            entity = null;
            response.close();

            log.warn("Received session-idle-timeout error from Fusion Session API, re-trying to establish a new session to " + sessionKey);
            try {
              clearCookieForHost(sessionHost);
            } catch (Exception exc) {
              log.warn("Failed to clear session cookie for " + sessionHost + " due to: " + exc);
            }

            response = httpClient.execute(postRequest, context);
            entity = response.getEntity();
            statusCode = response.getStatusLine().getStatusCode();
            if (statusCode != 200 && statusCode != 201 && statusCode != 204) {
              body = extractResponseBodyText(entity);
              throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
                      "POST credentials to Fusion Session API [" + sessionApi + "] failed due to: " +
                              response.getStatusLine() + ": " + body);
            }
          }
        }
      } finally {
        EntityUtils.consumeQuietly(entity);
        if (response != null) response.close();
      }
      log.info("Established secure session with Fusion Session API on " + sessionKey + " for user " + user + " in realm " + realm);
    }

    fusionSession.sessionEstablishedAt = System.nanoTime();
    fusionSession.docsSentMeter = getMeterByHost("Docs Sent to Fusion", sessionKey);
    fusionSession.id = sessionKey;

    return fusionSession;
  }

  protected synchronized void clearCookieForHost(String sessionHost) throws Exception {
    Cookie sessionCookie = null;
    for (Cookie cookie : cookieStore.getCookies()) {
      String cookieDomain = cookie.getDomain();
      if (cookieDomain != null) {
        if (sessionHost.equals(cookieDomain) ||
                sessionHost.indexOf(cookieDomain) != -1 ||
                cookieDomain.indexOf(sessionHost) != -1) {
          sessionCookie = cookie;
          break;
        }
      }
    }

    if (sessionCookie != null) {
      BasicClientCookie httpCookie = new BasicClientCookie(sessionCookie.getName(), sessionCookie.getValue());
      httpCookie.setExpiryDate(new Date(0));
      httpCookie.setVersion(1);
      httpCookie.setPath(sessionCookie.getPath());
      httpCookie.setDomain(sessionCookie.getDomain());
      cookieStore.addCookie(httpCookie);
    }

    cookieStore.clearExpired(new Date()); // this should clear the cookie
  }

  protected String getSessionKey(String url) throws Exception {
    if (!url.startsWith("http://") && !url.startsWith("https://"))
      url = "http://"+url;
    URL javaUrl = new URL(url);
    return javaUrl.getProtocol() + "://" + javaUrl.getHost() + ":" + javaUrl.getPort();
  }

  protected FusionSession getSession(String url, int requestId) throws Exception {
    String sessionKey = getSessionKey(url);
    FusionSession fusionSession;
    synchronized (this) {
      fusionSession = sessions.get(sessionKey);

      // ensure last request within the session timeout period, else reset the session
      long currTime = System.nanoTime();
      if (fusionSession == null || (currTime - fusionSession.sessionEstablishedAt) > maxNanosOfInactivity) {
        log.info("Fusion session is likely expired (or soon will be) for " + url + ", " +
                "pre-emptively re-setting this session before processing request " + requestId);
        fusionSession = resetSession(sessionKey);
        if (fusionSession == null)
          throw new IllegalStateException("Failed to re-connect to " + url +
                  " after session loss when processing request " + requestId);
      }
    }
    return fusionSession;
  }

  protected synchronized FusionSession resetSession(String sessionKey) throws Exception {
    // reset the "context" object for the HttpContext for this endpoint
    FusionSession fusionSession;
    try {
      fusionSession = establishSession(sessionKey, fusionUser, fusionPass, fusionRealm);
      sessions.put(fusionSession.id, fusionSession);
    } catch (Exception exc) {
      log.error("Failed to re-establish session with Fusion at " + sessionKey + " due to: " + exc);
      sessions.remove(sessionKey);
      fusionSession = null;
    }
    return fusionSession;
  }

  public HttpClient getHttpClient() {
    return httpClient;
  }

  public String getAvailableServer() {
    try {
      return getLbServer(getAvailableServers());
    } catch (Exception exc) {
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(exc);
      }
    }
  }

  protected String getLbServer(List<String> list) {
    int num = list.size();
    if (num == 0)
      return null;

    return list.get((num > 1) ? random.nextInt(num) : 0);
  }

  public ArrayList<String> getAvailableServers() throws Exception {
    ArrayList<String> mutable;
    synchronized (this) {
      mutable = new ArrayList<>(sessions.keySet());
    }

    if (mutable.isEmpty()) {
      // completely hosed ... try to re-establish all sessions
      synchronized (this) {
        try {
          Thread.sleep(2000);
        } catch (InterruptedException ie) {
          Thread.interrupted();
        }

        sessions = establishSessions(originalHostAndPortList, fusionUser, fusionPass, fusionRealm);
        mutable = new ArrayList<>(sessions.keySet());
      }
      if (mutable.isEmpty())
        throw new IllegalStateException("No available endpoints! " +
                "Check log for previous errors as to why there are no more endpoints available. This is a fatal error.");
    }

    return mutable;
  }

  public void postBatchToPipeline(String pipelinePath, List docs) throws Exception {
    postBatchToPipeline(pipelinePath, docs, PIPELINE_DOC_TYPE);
  }

  public void postBatchToPipeline(String pipelinePath, List docs, String contentType) throws Exception {

    int numDocs = docs.size();

    if (!pipelinePath.startsWith("/"))
      pipelinePath = "/" + pipelinePath;

    int requestId = requestCounter.incrementAndGet();

    ArrayList<String> mutable = getAvailableServers();
    if (mutable.size() > 1) {
      Exception lastExc = null;

      // try all the endpoints until success is reached ... or we run out of endpoints to try ...
      while (!mutable.isEmpty()) {
        String hostAndPort = getLbServer(mutable);
        if (hostAndPort == null) {
          // no more endpoints available ... fail
          if (lastExc != null) {
            log.error("No more hosts available to retry failed request (" + requestId + ")! raising last seen error: " + lastExc);
            throw lastExc;
          } else {
            throw new RuntimeException("No Fusion hosts available to process request " + requestId + "! Check logs for previous errors.");
          }
        }

        if (log.isDebugEnabled())
          log.debug("POSTing batch of " + numDocs + " input docs to " + hostAndPort + pipelinePath + " as request " + requestId);

        Exception retryAfterException =
                postDocsToPipelineWithRetry(hostAndPort, pipelinePath, docs, mutable, lastExc, requestId, contentType);
        if (retryAfterException == null) {
          lastExc = null;
          break; // request succeeded ...
        }

        lastExc = retryAfterException; // try next hostAndPort (if available) after seeing an exception
      }

      if (lastExc != null) {
        // request failed and we exhausted the list of endpoints to try ...
        log.error("Failing request " + requestId + " due to: " + lastExc);
        throw lastExc;
      }

    } else {
      String hostAndPort = getLbServer(mutable);
      if (log.isDebugEnabled())
        log.debug("POSTing batch of " + numDocs + " input docs to " + hostAndPort + pipelinePath + " as request " + requestId);

      Exception exc = postDocsToPipelineWithRetry(hostAndPort, pipelinePath, docs, mutable, null, requestId, contentType);
      if (exc != null)
        throw exc;
    }
  }

  protected Exception postDocsToPipelineWithRetry(String hostAndPort,
                                                  String pipelinePath,
                                                  List docs,
                                                  ArrayList<String> mutable,
                                                  Exception lastExc,
                                                  int requestId,
                                                  String contentType)
          throws Exception {
    String url = hostAndPort + pipelinePath;
    Exception retryAfterException = null;
    try {
      postDocsToPipeline(hostAndPort, pipelinePath, docs, requestId, contentType);
      if (lastExc != null)
        log.info("Re-try request " + requestId + " to " + url + " succeeded after seeing a " + lastExc.getMessage());
    } catch (Exception exc) {
      log.warn("Failed to send request " + requestId + " to '" + url + "' due to: " + exc);
      if (mutable.size() > 1) {
        // try another hostAndPort but update the cloned list to avoid re-hitting the one having an error
        if (log.isDebugEnabled())
          log.debug("Will re-try failed request " + requestId + " on next host in the list");

        mutable.remove(hostAndPort);
        retryAfterException = exc;
      } else {
        // no other endpoints to try ... brief wait and then retry
        log.warn("No more Fusion servers available to try ... will retry to send request " + requestId + " to " + url + " after waiting 1 sec");
        try {
          Thread.sleep(1000);
        } catch (InterruptedException ignore) {
          Thread.interrupted();
        }
        // note we want the exception to propagate from here up the stack since we re-tried and it didn't work
        postDocsToPipeline(hostAndPort, pipelinePath, docs, requestId, contentType);
        log.info("Re-try request " + requestId + " to " + url + " succeeded");
        retryAfterException = null; // return success condition
      }
    }

    return retryAfterException;
  }

  private class JacksonContentProducer implements ContentProducer {

    ObjectMapper mapper;
    Object jsonObj;

    JacksonContentProducer(ObjectMapper mapper, Object jsonObj) {
      this.mapper = mapper;
      this.jsonObj = jsonObj;
    }

    public void writeTo(OutputStream outputStream) throws IOException {
      mapper.writeValue(outputStream, jsonObj);
    }
  }

  private class CsvContentProducer implements ContentProducer {

    List docs;

    CsvContentProducer(List docs) {
      this.docs = docs;
    }

    public void writeTo(OutputStream outputStream) throws IOException {
      try (OutputStreamWriter osw = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8)) {
        CSVPrinter csvPrinter = new CSVPrinter(osw, CSVFormat.DEFAULT);
        for (Object doc : docs) {
          if (doc instanceof Map) {
            Map<String,Object> map = (Map<String,Object>)doc;
            csvPrinter.printRecord(map.values());
          } else {
            csvPrinter.print(doc);
            csvPrinter.println();
          }
        }
        csvPrinter.flush();
        csvPrinter.close();
      }
    }
  }


  public void postDocsToPipeline(String hostAndPort, String pipelinePath, List docs, int requestId, String contentType) throws Exception {
    FusionSession fusionSession = getSession(hostAndPort, requestId);
    String postUrl = hostAndPort + pipelinePath;
    if (postUrl.indexOf("?") != -1) {
      postUrl += "&echo=false";
    } else {
      postUrl += "?echo=false";
    }

    HttpPost postRequest = new HttpPost(postUrl);
    ContentProducer cp = newContentProducer(contentType, docs);
    EntityTemplate et = new EntityTemplate(cp);
    et.setContentType(contentType);
    et.setContentEncoding(StandardCharsets.UTF_8.name());
    postRequest.setEntity(et);

    HttpEntity entity = null;
    CloseableHttpResponse response = null;
    try {

      HttpClientContext context = null;
      if (isKerberos) {
        response = httpClient.execute(postRequest);
      } else {
        context = HttpClientContext.create();
        if (cookieStore != null) {
          context.setCookieStore(cookieStore);
        }
        response = httpClient.execute(postRequest, context);
      }
      entity = response.getEntity();

      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == 401) {
        // unauth'd - session probably expired? retry to establish
        log.warn("Unauthorized error (401) when trying to send request " + requestId +
                " to Fusion at " + hostAndPort + ", will re-try to establish session");

        // re-establish the session and re-try the request
        EntityUtils.consumeQuietly(entity);
        entity = null;
        response.close();

        synchronized (this) {
          fusionSession = resetSession(hostAndPort);
          if (fusionSession == null)
            throw new IllegalStateException("After re-establishing session when processing request " +
                    requestId + ", hostAndPort " + hostAndPort + " is no longer active! Try another hostAndPort.");
        }

        log.info("Going to re-try request " + requestId + " after session re-established with " + hostAndPort);
        if (isKerberos) {
          response = httpClient.execute(postRequest);
        } else {
          response = httpClient.execute(postRequest, context);
        }
        entity = response.getEntity();
        statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 200 || statusCode == 204) {
          log.info("Re-try request " + requestId + " after session timeout succeeded for: " + hostAndPort);
        } else {
          raiseFusionServerException(hostAndPort, entity, statusCode, response, requestId);
        }
      } else if (statusCode != 200 && statusCode != 204) {
        raiseFusionServerException(hostAndPort, entity, statusCode, response, requestId);
      } else {
        // OK!
        if (fusionSession != null && fusionSession.docsSentMeter != null)
          fusionSession.docsSentMeter.mark(docs.size());
      }
    } finally {
      EntityUtils.consumeQuietly(entity);
      if (response != null) response.close();
    }
  }

  protected ContentProducer newContentProducer(String contentType, List docs) {
    if ("text/csv".equals(contentType)) {
      return new CsvContentProducer(docs);
    } else if (PIPELINE_DOC_TYPE.equals(contentType) || "application/json".equals(contentType)) {
      return new JacksonContentProducer(jsonObjectMapper, docs);
    } else {
      throw new IllegalArgumentException("Content type "+contentType+" not supported! Use text/csv or "+PIPELINE_DOC_TYPE);
    }
  }

  public QueryResponse queryFusion(String queryPipelinePath, SolrQuery query) throws Exception {
    query.set("wt", "xml");

    String queryPath = queryPipelinePath+"?"+query;

    QueryResponse qr = null;
    int requestId = requestCounter.incrementAndGet();
    ArrayList<String> mutable = getAvailableServers();
    if (mutable.size() > 1) {
      Exception lastExc = null;

      // try all the endpoints until success is reached ... or we run out of endpoints to try ...
      while (!mutable.isEmpty()) {
        String hostAndPort = getLbServer(mutable);
        if (hostAndPort == null) {
          // no more endpoints available ... fail
          if (lastExc != null) {
            log.error("No more hosts available to retry failed request (" + requestId + ")! raising last seen error: " + lastExc);
            throw lastExc;
          } else {
            throw new RuntimeException("No Fusion hosts available to process request " + requestId + "! Check logs for previous errors.");
          }
        }

        Exception retryAfterException = null;
        try {
          qr = sendQuery(hostAndPort, queryPath, requestId);
          retryAfterException = null;
        } catch (Exception exc) {
          retryAfterException = exc;
        }

        if (retryAfterException == null) {
          lastExc = null;
          break; // request succeeded ...
        }

        lastExc = retryAfterException; // try next hostAndPort (if available) after seeing an exception
      }

      if (lastExc != null) {
        // request failed and we exhausted the list of endpoints to try ...
        log.error("Failing request " + requestId + " due to: " + lastExc);
        throw lastExc;
      }

    } else {
      String hostAndPort = getLbServer(mutable);
      qr = sendQuery(hostAndPort, queryPath, requestId);
    }
    return qr;
  }

  public SearchResponse queryFusionJson(String queryPipelinePath, SolrQuery query) throws Exception {
    query.set("wt", "json");

    String queryPath = queryPipelinePath+"?"+query;

    SearchResponse qr = null;
    int requestId = requestCounter.incrementAndGet();
    ArrayList<String> mutable = getAvailableServers();
    if (mutable.size() > 1) {
      Exception lastExc = null;

      // try all the endpoints until success is reached ... or we run out of endpoints to try ...
      while (!mutable.isEmpty()) {
        String hostAndPort = getLbServer(mutable);
        if (hostAndPort == null) {
          // no more endpoints available ... fail
          if (lastExc != null) {
            log.error("No more hosts available to retry failed request (" + requestId + ")! raising last seen error: " + lastExc);
            throw lastExc;
          } else {
            throw new RuntimeException("No Fusion hosts available to process request " + requestId + "! Check logs for previous errors.");
          }
        }

        Exception retryAfterException = null;
        try {
          qr = sendQueryAndParserAsJson(hostAndPort, queryPath, requestId);
          retryAfterException = null;
        } catch (Exception exc) {
          retryAfterException = exc;
        }

        if (retryAfterException == null) {
          lastExc = null;
          break; // request succeeded ...
        }

        lastExc = retryAfterException; // try next hostAndPort (if available) after seeing an exception
      }

      if (lastExc != null) {
        // request failed and we exhausted the list of endpoints to try ...
        log.error("Failing request " + requestId + " due to: " + lastExc);
        throw lastExc;
      }

    } else {
      String hostAndPort = getLbServer(mutable);
      qr = sendQueryAndParserAsJson(hostAndPort, queryPath, requestId);
    }
    return qr;
  }

  protected SearchResponse sendQueryAndParserAsJson(String hostAndPort, String query, int requestId) throws Exception {
    HttpGet getReq = new HttpGet(hostAndPort+query);
    HttpEntity getResp = sendRequestToFusion(getReq, true, requestId);
    JsonResponseParser responseParser = new JsonResponseParser(jsonObjectMapper);
    InputStream inputStream = null;
    SearchResponse qr = null;
    try {
//      reader = new BufferedReader(new InputStreamReader(getResp.getContent()));
      inputStream = getResp.getContent();
      qr = responseParser.processResponse(inputStream, "UTF-8");
    } finally {

      if (inputStream != null) {
        try {
          inputStream.close();
        } catch (Exception ignore) {}
      }
      if (getResp instanceof HttpEntityAndResponse) {
        ((HttpEntityAndResponse)getResp).close();
      } else {
        EntityUtils.consumeQuietly(getResp);
      }
    }
    return qr;
  }

  protected QueryResponse sendQuery(String hostAndPort, String query, int requestId) throws Exception {
    HttpGet getReq = new HttpGet(hostAndPort+query);
    HttpEntity getResp = sendRequestToFusion(getReq, true, requestId);
    XMLResponseParser responseParser = new XMLResponseParser();
    Reader reader = null;
    QueryResponse qr = null;
    try {
      reader = new BufferedReader(new InputStreamReader(getResp.getContent()));
      NamedList<Object> parsedResp = responseParser.processResponse(reader);
      qr = new QueryResponse(parsedResp, null);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (Exception ignore) {}
      }
      if (getResp instanceof HttpEntityAndResponse) {
        ((HttpEntityAndResponse)getResp).close();
      } else {
        EntityUtils.consumeQuietly(getResp);
      }
    }
    return qr;
  }


  public static class HttpEntityAndResponse implements HttpEntity, Closeable {

    protected HttpEntity delegate;
    protected CloseableHttpResponse httpResponse;

    public HttpEntityAndResponse(HttpEntity delegate, CloseableHttpResponse httpResponse) {
      this.delegate = delegate;
      this.httpResponse = httpResponse;
    }

    public void close() {
      if (delegate != null) {
        EntityUtils.consumeQuietly(delegate);
      }

      if (httpResponse != null) {
        try {
          httpResponse.close();
        } catch (Exception ignore){}
      }
    }

    @Override
    public boolean isRepeatable() {
      return delegate.isRepeatable();
    }

    @Override
    public boolean isChunked() {
      return delegate.isChunked();
    }

    @Override
    public long getContentLength() {
      return delegate.getContentLength();
    }

    @Override
    public Header getContentType() {
      return delegate.getContentType();
    }

    @Override
    public Header getContentEncoding() {
      return delegate.getContentEncoding();
    }

    @Override
    public InputStream getContent() throws IOException, UnsupportedOperationException {
      return delegate.getContent();
    }

    @Override
    public void writeTo(OutputStream outputStream) throws IOException {
      delegate.writeTo(outputStream);
    }

    @Override
    public boolean isStreaming() {
      return delegate.isStreaming();
    }

    @Override
    public void consumeContent() throws IOException {
      delegate.consumeContent();
    }
  }

  public HttpEntity sendRequestToFusion(HttpUriRequest httpRequest) throws Exception {
    return sendRequestToFusion(httpRequest, true);
  }

  public HttpEntity sendRequestToFusion(HttpUriRequest httpRequest, boolean retry) throws Exception {
    return sendRequestToFusion(httpRequest, retry, requestCounter.incrementAndGet());
  }

  public HttpEntity sendRequestToFusion(HttpUriRequest httpRequest, boolean retry, int requestId) throws Exception {
    String endpoint = httpRequest.getRequestLine().getUri();
    URL url = httpRequest.getURI().toURL();
    String hostAndPort = url.getProtocol()+"://"+url.getHost()+":"+url.getPort();
    FusionSession fusionSession = getSession(hostAndPort, requestId);

    HttpEntity entity = null;
    CloseableHttpResponse response = null;
    HttpClientContext context = null;

    if (log.isDebugEnabled()) {
      log.debug("Sending "+httpRequest.getMethod()+" request to: "+endpoint);
    }

    if (isKerberos) {
      response = httpClient.execute(httpRequest);
    } else {
      context = HttpClientContext.create();
      if (cookieStore != null) {
        context.setCookieStore(cookieStore);
      }
      response = httpClient.execute(httpRequest, context);
    }

    entity = response.getEntity();
    int statusCode = response.getStatusLine().getStatusCode();
    if (log.isDebugEnabled()) {
      log.debug(httpRequest.getMethod()+" request to "+endpoint+" returned: "+statusCode);
    }

    if (!retry) {
      if (statusCode == 200 || statusCode == 204) {
        // the requesting client needs to close the entity and HttpResponse object
        return new HttpEntityAndResponse(entity, response);
      } else {
        try {
          raiseFusionServerException(endpoint, entity, statusCode, response, requestId);
        } finally {
          EntityUtils.consumeQuietly(entity);
          if (response != null) response.close();
        }
      }
    }

    if (statusCode == 401) {
      // unauth'd - session probably expired? retry to establish
      log.warn("Unauthorized error (401) when trying to send request " + requestId +
              " to Fusion at " + endpoint + ", will re-try to establish session");

      // re-establish the session and re-try the request
      EntityUtils.consumeQuietly(entity);
      response.close();

      String sessionKey = fusionSession.id;
      synchronized (this) {
        fusionSession = resetSession(sessionKey);
        if (fusionSession == null)
          throw new IllegalStateException("After re-establishing session when processing request " +
                  requestId + ", Fusion host " + sessionKey + " is no longer active! Try another server.");
      }

      log.info("Going to re-try request " + requestId + " after session re-established with " + sessionKey);

      if (isKerberos) {
        response = httpClient.execute(httpRequest);
      } else {
        response = httpClient.execute(httpRequest, context);
      }
      entity = response.getEntity();
      statusCode = response.getStatusLine().getStatusCode();
      if (statusCode == 200 || statusCode == 204) {
        log.info("Re-try request " + requestId + " after session timeout succeeded for: " + endpoint);
      } else {
        try {
          raiseFusionServerException(endpoint, entity, statusCode, response, requestId);
        } finally {
          EntityUtils.consumeQuietly(entity);
          if (response != null) response.close();
        }
      }
    } else if (statusCode != 200 && statusCode != 204) {
      try {
        raiseFusionServerException(endpoint, entity, statusCode, response, requestId);
      } finally {
        EntityUtils.consumeQuietly(entity);
        if (response != null) response.close();
      }
    }

    return new HttpEntityAndResponse(entity, response);
  }

  protected void raiseFusionServerException(String endpoint, HttpEntity entity, int statusCode, HttpResponse response, int requestId) {
    String body = extractResponseBodyText(entity);
    throw new SolrException(SolrException.ErrorCode.getErrorCode(statusCode),
            "Request " + requestId + " to [" + endpoint + "] failed due to: (" + statusCode + ")" + response.getStatusLine() + ": " + body);
  }

  public static String extractResponseBodyText(HttpEntity entity) {
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
        log.warn("Failed to read response body due to: " + ignore);
      } finally {
        if (reader != null) {
          try {
            reader.close();
          } catch (Exception ignore) {
          }
        }
      }
    }
    return body.toString();
  }

  public synchronized void shutdown() {
    if (sessions != null) {
      sessions.clear();
      sessions = null;
    }

    if (httpClient != null) {
      log.info("Closing HttpClient.");
      try {
        httpClient.close();
      } catch (IOException e) {
        log.warn("Failed to close httpClient object due to: " + e);
      } finally {
        httpClient = null;
      }
    } else {
      log.error("Already shutdown.");
    }
  }
}
