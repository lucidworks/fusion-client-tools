package com.lucidworks.client;

import com.github.tomakehurst.wiremock.http.Request;
import com.github.tomakehurst.wiremock.http.RequestListener;
import com.github.tomakehurst.wiremock.http.Response;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.apache.hadoop.util.hash.Hash;
import org.apache.solr.common.SolrInputDocument;
import org.codehaus.jackson.JsonNode;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class TestFusionDocumentWriter {

  private static final Boolean useWireMockRule = false;

  private Log log = LogFactory.getLog(FusionDocumentWriter.class);

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8089); // No-args constructor defaults to port 8080

  @Test
  public void testFusionDocumentWriter() throws Exception {
    String fusionUrl;
    String fusionSolrProxy;
    if (useWireMockRule) {
      fusionUrl = "http://localhost:8089/api/pipeline";
      fusionSolrProxy = "http://localhost:8089/api/solr";
    } else {
      // TODO: The following is very specific to a local Fusion install. Should do something to at least permit
      //       users to specify the collection, the indexing pipeline, etc. to be used instead of this hard coded
      //       URL when connecting to a real Fusion instance.
      fusionUrl = "http://localhost:8764/api/apollo/index-pipelines/scottsCollection-default/collections/agentCollection/index";
      fusionSolrProxy = "http://localhost:8764/api/apollo/solr/agentCollection";
    }
    String fusionUser = "admin";
    String fusionPass = "password123";
    String fusionRealm = "native";

    Map<String,String> config = new HashMap<String,String>();
    config.put("fusion.pipeline", fusionUrl);
    config.put("fusion.solrproxy", fusionSolrProxy);
    config.put("fusion.user", fusionUser);
    config.put("fusion.pass", fusionPass);
    config.put("fusion.realm", fusionRealm);

    if (useWireMockRule) {
      // mock out the Pipeline API
      stubFor(post(urlEqualTo("/api/pipeline")).willReturn(aResponse().withStatus(200)));

      // mock out the Session API
      stubFor(post(urlEqualTo("/api/session?realmName="+fusionRealm)).willReturn(aResponse().withStatus(200)));

      // mock out the Solr proxy
      stubFor(post(urlEqualTo("/api/solr")).willReturn(aResponse().withStatus(200)));
    }
    FusionDocumentWriter docWriter = new FusionDocumentWriter("agentCollection" /* indexName */, config);

    if (useWireMockRule) {
      // register a callback to validate the request that came into our mock pipeline endpoint
      wireMockRule.addMockServiceRequestListener(new RequestListener() {
        public void requestReceived(Request request, Response response) {
          validateRequest(request);
        }
      });
    }

    log.info("Adding one document from buildInputDocs().");
    docWriter.add(1, buildInputDocs(1, 0));
    log.info("\nAdding two documents from buildAtomicUpdateDocs().");
    docWriter.add(1, buildAtomicUpdateDocs(2, 0));

    log.info("\n\nStarting homoginized list of documents...");
    Map<String, SolrInputDocument> homoginizedDocuments = buildInputDocs(1,0);
    homoginizedDocuments.putAll(buildAtomicUpdateDocs(2, 2));
    homoginizedDocuments.putAll(buildInputDocs(1, 10));
    homoginizedDocuments.putAll(buildAtomicUpdateDocs(1, 13));
    homoginizedDocuments.putAll(buildInputDocs(2, 20));
    log.info("\n\nAdding homoginized documents. Total documents:[" + homoginizedDocuments.toString() + "]");
    docWriter.add(1, homoginizedDocuments);

    log.info("======================================================================================");
    log.info("\n\nAdding nested documents.");
    docWriter.add(1, buildNestedDocs(4, 1));
    log.info("\n\nFinished adding nested documents.");
    log.info("======================================================================================");

    // End of adding nested documents.
    log.info("Done adding documents.");

    log.info("Now, delete by ID where the ID is 'product021'");
    List<String> idsToDelete = new ArrayList<String>();
    idsToDelete.add("product021");
    docWriter.deleteById(1, idsToDelete);

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

  protected Map<String, SolrInputDocument> buildInputDocs(int numDocs, int startVal) {
    Map<String, SolrInputDocument> inputDocumentMap = new HashMap<String,SolrInputDocument>();
    for (int d=startVal; d < numDocs+startVal; d++) {
      SolrInputDocument doc = new SolrInputDocument();
      String docId = "doc"+d;
      doc.setField("id", docId);
      doc.setField("name_s", "foo "+d);
      inputDocumentMap.put(docId, doc);
    }
    return inputDocumentMap;
  }


  /**
   * shs: Generate atomic update documents for validating the atomic update portion of the add method.
   * @param numDocs   The number of atomic update documents to be created.
   * @param startVal  The initial starting value of incrementer. By having this parameter, we can create lists of
   *                  documents where normal documents and atomic update documents are intermingled. This will permit
   *                  the verification of the parser that seperates atomic update documents from regular documents.
   * @return
   */
  protected Map<String, SolrInputDocument> buildAtomicUpdateDocs(int numDocs, int startVal) {
    Map<String, SolrInputDocument> inputDocumentMap = new HashMap<String,SolrInputDocument>();
    Map<String,String> atomicUpdateMap = new HashMap<String, String>();
    for (int d=startVal; d < numDocs+startVal; d++) {
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
   * shs: Generate nested documents for validating the nested document portion of the add method.
   * @param numDocs   The number of atomic update documents to be created.
   * @param startVal  The initial starting value of incrementer. By having this parameter, we can create lists of
   *                  documents where normal documents and atomic update documents are intermingled. This will permit
   *                  the verification of the parser that seperates atomic update documents from regular documents.
   * @TODO This would be better with a generalized processor that would create nested documents to a specific nesting
   *       depth. Currently any number of documents will be created; however, those documents will follow this pattern:
   *       Odd numbered document will have a parent document with three children. The third child also has three children.
   *       Even numbered documents have a parent document with two child documents.
   * @return
   */
  protected Map<String, SolrInputDocument> buildNestedDocs(int numDocs, int startVal) {
    Map<String, SolrInputDocument> inputDocumentMap = new HashMap<String, SolrInputDocument>();
    for (int d = startVal; d < numDocs + startVal; d++) {


      // Adding nested documents - adopted from M Bennett's SolrJ Nested Docs project
      /***
       Equivalent Solr XML doc:
       <add>
       <doc>
       <field name="id">product01</field>
       <field name="name_s">car</field>
       <field name="content_type_s">product</field>
       <doc>
       <field name="id">part01</field>
       <field name="name_s">wheels</field>
       <field name="content_type_s">part</field>
       </doc>
       <doc>
       <field name="id">part02</field>
       <field name="name_s">engine</field>
       <field name="doctype">part</field>
       </doc>
       <doc>
       <field name="id">part03</field>
       <field name="name_s">brakes</field>
       <field name="content_type_s">part</field>
       <doc>
       <field  name="id">subpart01</field>
       <field name="name_s">brakePads</field>
       <field name="content_type_s">component</field>
       </doc>
       <doc>
       <field  name="id">subpart02</field>
       <field name="name_s">calipers</field>
       <field name="content_type_s">component</field>
       </doc>
       <doc>
       <field  name="id">subpart03</field>
       <field name="name_s">drums</field>
       <field name="content_type_s">component</field>
       </doc>
       </doc>
       </doc>
       <doc>
       <field name="id">product02</field>
       <field name="name_s">truck</field>
       <field name="content_type_s">product</field>
       <doc>
       <field name="id">part04</field>
       <field name="name_s">wheels</field>
       <field name="content_type_s">part</field>
       </doc>
       <doc>
       <field name="id">part05</field>
       <field name="name_s">flaps</field>
       <field name="doctype">part</field>
       </doc>
       <doc>
       </add>
       ***/

      if ((d % 2) == 0) {

        // Parent Doc 1
        SolrInputDocument product01 = new SolrInputDocument();
        product01.addField("id", "product01" + startVal);
        product01.addField("name_s", "car");
        product01.addField("content_type_s", "product");

        // 3 Children
        SolrInputDocument part01 = new SolrInputDocument();
        part01.addField("id", product01.getFieldValue("id")+ "|||" + "part01" + startVal);
        part01.addField("name_s", "wheels");
        part01.addField("content_type_s", "part");
        SolrInputDocument part02 = new SolrInputDocument();
        part02.addField("id", product01.getFieldValue("id")+ "|||" + "part02" + startVal);
        part02.addField("name_s", "engine");
        part02.addField("content_type_s", "part");
        SolrInputDocument part03 = new SolrInputDocument();
        part03.addField("id", product01.getFieldValue("id")+ "|||" + "part03" + startVal);
        part03.addField("name_s", "brakes");
        part03.addField("content_type_s", "part");

        // 3 Grand Children
        SolrInputDocument subpart01 = new SolrInputDocument();
        subpart01.addField("id", part03.getFieldValue("id")+ "|||" + "subpart01" + startVal);
        subpart01.addField("name_s", "brakePads");
        subpart01.addField("content_type_s", "component");
        SolrInputDocument subpart02 = new SolrInputDocument();
        subpart02.addField("id", part03.getFieldValue("id")+ "|||" + "subpart02" + startVal);
        subpart02.addField("name_s", "calipers");
        subpart02.addField("content_type_s", "component");
        SolrInputDocument subpart03 = new SolrInputDocument();
        subpart03.addField("id", part03.getFieldValue("id")+ "|||" + "subpart03" + startVal);
        subpart03.addField("name_s", "drums");
        subpart03.addField("content_type_s", "component");

        // Add grandchildren to parent.
        part03.addChildDocument(subpart01);
        part03.addChildDocument(subpart02);
        part03.addChildDocument(subpart03);

        // Add children to parent
        product01.addChildDocument(part01);
        product01.addChildDocument(part02);
        product01.addChildDocument(part03);
        inputDocumentMap.put((String) product01.getFieldValue("id"), product01);

      } else {

        // Parent Doc 2 with 2 children
        SolrInputDocument product02 = new SolrInputDocument();
        product02.addField("id", "product02" + startVal);
        product02.addField("name_s", "truck");
        product02.addField("content_type_s", "product");
        SolrInputDocument part04 = new SolrInputDocument();
        part04.addField("id", product02.getFieldValue("id")+ "|||" + "part04" + startVal);
        part04.addField("name_s", "wheels");
        part04.addField("content_type_s", "part");
        SolrInputDocument part05 = new SolrInputDocument();
        part05.addField("id", product02.getFieldValue("id")+ "|||" + "part05" + startVal);
        part05.addField("name_s", "flaps");
        part05.addField("content_type_s", "part");
        product02.addChildDocument(part04);
        product02.addChildDocument(part05);
        inputDocumentMap.put((String) product02.getFieldValue("id"), product02);
      }
    }

    return inputDocumentMap;
  }
}
