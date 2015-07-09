package com.lucidworks.client;

import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestListener;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.JsonNode;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class TestFusionDocumentWriter {

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8089); // No-args constructor defaults to port 8080

  @Test
  public void testFusionDocumentWriter() throws Exception {

    String fusionUrl = "http://localhost:8089/api/pipeline";
    String fusionSolrProxy = "http://localhost:8089/api/solr";
    String fusionUser = "admin";
    String fusionPass = "password123";
    String fusionRealm = "default";

    Map<String,String> config = new HashMap<String,String>();
    config.put("fusion.pipeline", fusionUrl);
    config.put("fusion.solrproxy", fusionSolrProxy);
    config.put("fusion.user", fusionUser);
    config.put("fusion.pass", fusionPass);
    config.put("fusion.realm", fusionRealm);

    // mock out the Pipeline API
    stubFor(post(urlEqualTo("/api/pipeline")).willReturn(aResponse().withStatus(200)));

    // mock out the Session API
    stubFor(post(urlEqualTo("/api/session?realmName="+fusionRealm)).willReturn(aResponse().withStatus(200)));

    // mock out the Solr proxy
    stubFor(post(urlEqualTo("/api/solr")).willReturn(aResponse().withStatus(200)));

    FusionDocumentWriter docWriter = new FusionDocumentWriter("test" /* indexName */, config);

    // register a callback to validate the request that came into our mock pipeline endpoint
    wireMockRule.addMockServiceRequestListener(new RequestListener() {
      public void requestReceived(Request request, Response response) {
        validateRequest(request);
      }
    });

    docWriter.add(1, buildInputDocs(1));
  }

  /**
   * Verify the JSON document that was posted to the pipeline by the FusionDocumentWriter.
   */
  protected void validateRequest(Request request) {
    if (request.getUrl().endsWith("/pipeline")) {
      String body = request.getBodyAsString();
      ObjectMapper om = new ObjectMapper();
      try {
        JsonNode tree = om.readTree(body);
        if (!tree.isArray() || tree.size() != 1)
          fail("Expected JSON list containing one object in request to the pipeline endpoint, but got: " + tree);
        JsonNode doc = tree.get(0);
        assertEquals("doc0", doc.get("id").getTextValue());
        assertNotNull(doc.get("fields"));
        // TODO: could do more validation of the fields data here
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  protected Map<String, SolrInputDocument> buildInputDocs(int numDocs) {
    Map<String, SolrInputDocument> inputDocumentMap = new HashMap<String,SolrInputDocument>();
    for (int d=0; d < numDocs; d++) {
      SolrInputDocument doc = new SolrInputDocument();
      String docId = "doc"+d;
      doc.setField("id", docId);
      doc.setField("name_s", "foo "+d);
      inputDocumentMap.put(docId, doc);
    }
    return inputDocumentMap;
  }
}
