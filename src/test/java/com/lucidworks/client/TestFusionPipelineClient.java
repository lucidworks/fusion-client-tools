package com.lucidworks.client;

import com.github.tomakehurst.wiremock.junit.WireMockRule;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.tomakehurst.wiremock.client.WireMock.*;

public class TestFusionPipelineClient {
  private static final Boolean useWireMockRule = false;

  @Rule
  public WireMockRule wireMockRule = new WireMockRule(8089); // No-args constructor defaults to port 8080

  @Test
  public void testHappyPath() throws Exception {

    String fusionUrl;
    if (useWireMockRule) {
      fusionUrl = "http://localhost:8764/api/pipeline";
    } else {
      // TODO: The following is very specific to a local Fusion install. Should do something to at least permit
      //       users to specify the collection, the indexing pipeline, etc. to be used instead of this hard coded
      //       URL when connecting to a real Fusion instance.
      fusionUrl = "http://localhost:8764/api/apollo/index-pipelines/scottsCollection-default/collections/agentCollection/index";
    }
    String fusionUser = "admin";
    String fusionPass = "password123";
    String fusionRealm = "native";

    if (useWireMockRule) {
      // mock out the Pipeline API
      stubFor(post(urlEqualTo("/api/pipeline")).willReturn(aResponse().withStatus(200)));

      // mock out the Session API
      stubFor(post(urlEqualTo("/api/session?realmName=" + fusionRealm)).willReturn(aResponse().withStatus(200)));

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
