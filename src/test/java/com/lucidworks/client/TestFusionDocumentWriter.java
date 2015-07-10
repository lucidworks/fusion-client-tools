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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class TestFusionDocumentWriter {

  private Log log = LogFactory.getLog(FusionDocumentWriter.class);

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8089); // No-args constructor defaults to port 8080

  @Test
  public void testFusionDocumentWriter() throws Exception {
    // TODO Need to add nested documents to at least the "grandchild" level, meaning it needs to be more than just a
    //      single nest level, i.e. parent with child.

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

    log.info("Adding one document from buildInputDocs().");
    docWriter.add(1, buildInputDocs(1));
    log.info("Adding two documents from buildAtomicUpdateDocs().");
    docWriter.add(1, buildAtomicUpdateDocs(2));
    log.info("Done adding documents.");
  }

  protected Map<String, SolrInputDocument> buildAtomicUpdateDocs(int numDocs) {
    Map<String, SolrInputDocument> inputDocumentMap = new HashMap<String,SolrInputDocument>();
    Map<String,String> atomicUpdateMap = new HashMap<String, String>();
    for (int d=0; d < numDocs; d++) {
      SolrInputDocument doc = new SolrInputDocument();
      String docId = "doc"+d;
      doc.setField("id", docId);
      // Atomic Updates now...
      atomicUpdateMap.clear();
      // An 'add' atomic update
      atomicUpdateMap.put("add", "add"+d);
      doc.setField("add_s", atomicUpdateMap);
      // A set example
      atomicUpdateMap.clear();
      atomicUpdateMap.put("set", "This is a set value in document " + d + ".");
      doc.setField("set_s", atomicUpdateMap);
      // An increment example
      atomicUpdateMap.clear();
      atomicUpdateMap.put("inc", Integer.toString(d));
      doc.setField("inc_ti", atomicUpdateMap);
      inputDocumentMap.put(docId, doc);
    }
    return inputDocumentMap;
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
      // TODO The validation of a solr input document needs to be improved...currently there is no checking only an
      //      additional 'else' clause to detect documents being sent to Solr instead of the Fusion pipeline. Without
      //      this, the validation failed because it was expecting JSON documents.
    } else if (request.getUrl().endsWith("/solr")) {
      String body = request.getBodyAsString();
      log.info("Solr document(s) as string:[" + body + "]");
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
