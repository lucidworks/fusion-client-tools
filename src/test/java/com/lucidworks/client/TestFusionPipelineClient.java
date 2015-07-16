package com.lucidworks.client;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Rule;
import org.junit.Test;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class TestFusionPipelineClient {

  private static final String defaultWireMockRulePort = "8089";
  private static final String defaultFusionPort = "8764";
  private static final String defaultHost = "localhost";
  private static final String defaultCollection = "test";
  private static final String defaultFusionIndexingPipeline = "conn_solr";
  private static final String defaultFusionProxyBaseUrl = "api/apollo";
  private static final String defaultFusionIndexingPipelineUrlExtension = "index-pipelines";
  private static final String defaultFusionSessionApi = "/api/session?realmName=";
  private static final String defaultFusionUser = "admin";
  private static final String defaultFusionPass = "password123";
  private static final String defaultFusionRealm = "native";
  private static final String defaultFusionSolrProxyUrlExtension = "solr";

  private static String host;
  private static String port;
  private static String wireMockRulePort;
  private static String fusionCollection;
  private static String fusionIndexingPipeline;
  private static String fusionProxyBaseUrl;
  private static String fusionIndexingPipelineUrlExtension;
  private static String fusionSessionApi;
  private static String fusionUser;
  private static String fusionPass;
  private static String fusionRealm;
  private static String fusionSolrProxyUrlExtension;
  private static Boolean useWireMockRule;

  private static final Log log = LogFactory.getLog(FusionPipelineClient.class);

  static {

    try (InputStream in = new FileInputStream("properties/properties.xml")) {
      Properties prop = new Properties();
      prop.loadFromXML(in);

      useWireMockRule = Boolean.getBoolean(prop.getProperty("useWireMockRule"));
      if (useWireMockRule) {
        // Set host and port when using WireMockRules.
        host = prop.getProperty("wireMockRuleHost", defaultHost);
        port = prop.getProperty("wireMockRulePort", defaultWireMockRulePort);
        wireMockRulePort = port;
      } else {
        // Set host and port when connecting to Fusion.
        host = prop.getProperty("fusionHost", defaultHost);
        port = prop.getProperty("fusionApiPort", defaultFusionPort);
        wireMockRulePort = defaultWireMockRulePort;
      }

      // Set collection.
      fusionCollection = prop.getProperty("fusionCollection", defaultCollection);

      // Set the fusion indexing pipeline.
      fusionIndexingPipeline = prop.getProperty("fusionIndexingPipeline", defaultFusionIndexingPipeline);

      // Set the fusion proxy base URL.
      fusionProxyBaseUrl = prop.getProperty("fusionProxyBaseUrl", defaultFusionProxyBaseUrl);

      // Set the fusion indexing pipeline URL extension.
      fusionIndexingPipelineUrlExtension = prop.getProperty("fusionIndexingPipelineUrlExtension", defaultFusionIndexingPipelineUrlExtension);

      // Set the fusion session API.
      fusionSessionApi = prop.getProperty("fusionSessionApi", defaultFusionSessionApi);

      // Set the fusion user.
      fusionUser = prop.getProperty("fusionUser", defaultFusionUser);

      // Set the fusion password.
      fusionPass = prop.getProperty("fusionPass", defaultFusionPass);

      // Set the fusion realm.
      fusionRealm = prop.getProperty("fusionRealm", defaultFusionRealm);

      // Set the fusion solr proxy URL extension.
      fusionSolrProxyUrlExtension = prop.getProperty("fusionSolrProxyUrlExtension", defaultFusionSolrProxyUrlExtension);
    } catch (IOException e) {
      log.error("Error reading properties.xml file due to exception:[" + e.toString() + "]");
      throw new RuntimeException();
    }
  }

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(Integer.parseInt(wireMockRulePort)); // No-args constructor defaults to port 8080
//  public WireMockRule wireMockRule = new WireMockRule(8089); // No-args constructor defaults to port 8080

  @Test
  public void testHappyPath() throws Exception {

    String fusionHostAndPort = "http://" + host + ":" + port;
    String fusionPipelineUrlWithoutHostAndPort = "/" + fusionProxyBaseUrl + "/" + fusionIndexingPipelineUrlExtension + "/" +
            fusionIndexingPipeline + "/collections/" + fusionCollection + "/index";
    String fusionUrl = fusionHostAndPort + fusionPipelineUrlWithoutHostAndPort;
    String fusionSolrProxyWithoutHostAndPort = "/" + fusionSolrProxyUrlExtension + "/" + fusionCollection;

    if (useWireMockRule) {
      // mock out the Pipeline API
      //  stubFor(post(urlEqualTo("/api/apollo/index-pipelines")).willReturn(aResponse().withStatus(200)));
      stubFor(post(urlEqualTo(fusionPipelineUrlWithoutHostAndPort)).willReturn(aResponse().withStatus(200)));

      // mock out the Session API
      // stubFor(post(urlEqualTo("/api/session?realmName=" + fusionRealm)).willReturn(aResponse().withStatus(200)));
      stubFor(post(urlEqualTo(fusionSolrProxyWithoutHostAndPort + fusionRealm)).willReturn(aResponse().withStatus(200)));

    }
    FusionPipelineClient pipelineClient =
      new FusionPipelineClient(fusionUrl, fusionUser, fusionPass, fusionRealm);
    pipelineClient.postBatchToPipeline(buildDocs(1));
  }

  protected List<Map<String,Object>> buildDocs(int numDocs) {
    List<Map<String,Object>> docs = new ArrayList<Map<String,Object>>(numDocs);
    for (int n=0; n < numDocs; n++) {
      Map<String,Object> doc = new HashMap<String, Object>();
      doc.put("id", "doc"+n);
      doc.put("str_s", "str "+n);
      docs.add(doc);
    }
    return docs;
  }
}
