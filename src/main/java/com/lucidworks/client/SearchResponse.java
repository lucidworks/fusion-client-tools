package com.lucidworks.client;

import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import java.util.*;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
public class SearchResponse {
  public Response response;

  @JsonProperty("responseHeader")
  public ResponseHeader responseHeader;

  @JsonProperty("facet_counts")
  public FacetCountsCollection facetCounts;

  @JsonProperty("stats")
  public Stats stats;

  @Override
  public String toString() {
    return "SearchResponse{" +
        "response=" + response + "\n" +
        "responseHeader=" + responseHeader + "\n" +
        "facetCounts=" + facetCounts + "\n" +
        "stats=" + stats + "\n" +
        "debug=" + debug + "\n" +
        "highlighting=" + highlighting + "\n" +
        "fusionResponse=" + fusionResponse + "\n" +
        '}';
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Stats {
    @JsonProperty("stats_fields")
    public Map<String, Object> statsFields;
  }

  @JsonProperty("debug")
  public Map<String, Object> debug;

  @JsonProperty("highlighting")
  public Map<String, Object> highlighting;

  @JsonProperty("fusionResponse")
  public Map<String, Object> fusionResponse;

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Response {
    public long numFound;
    public long start;
    public double maxScore;
    public List<Map<String, Object>> docs;

    @Override
    public String toString() {
      return "Response{" +
          "numFound=" + numFound +
          ", start=" + start +
          ", maxScore=" + maxScore +
          ", docs=" + docs +
          '}';
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class FacetCountsCollection {
    @JsonProperty("facet_fields")
    private Map<String, Object> facetFields;

    @JsonProperty("facet_ranges")
    private Map<String, FacetCountsCollection.FacetRange> facetRanges;

    public List<FacetCountsCollection.FacetValue> getFacet(String field) {
      List<FacetCountsCollection.FacetValue> res = getFacetField(field);
      if (res.isEmpty()) {
        res = getFacetRange(field);
      }
      return res;
    }

    @JsonProperty("facet_fields")
    public Map<String, Object> getFacetFields() {
      return facetFields;
    }

    public List<FacetCountsCollection.FacetValue> getFacetField(String field) {
      List<FacetCountsCollection.FacetValue> facets = new ArrayList<FacetCountsCollection.FacetValue>();
      if (!facetFields.containsKey(field)) {
        return facets;
      }
      // this was written assuming that facetFields contains a map, but in recent versions of Solr at least it's an array.
      Object o = facetFields.get(field);
      if (o instanceof Map) {
        for (Map.Entry<String, Integer> entry : ((Map<String, Integer>) o).entrySet()) {
          facets.add(new FacetCountsCollection.FacetValue(entry.getKey(), entry.getValue()));
        }
      } else if (o instanceof Collection) {
        Iterator<Object> iter = ((Collection<Object>) o).iterator();
        while (iter.hasNext()) {
          // alternating string value and integer count.
          facets.add(new FacetCountsCollection.FacetValue(iter.next().toString(), (Integer) iter.next()));
        }
      }
      return facets;
    }

    @JsonProperty("facet_ranges")
    public Map<String, FacetCountsCollection.FacetRange> getFacetRanges() {
      return facetRanges;
    }

    public List<FacetCountsCollection.FacetValue> getFacetRange(String field) {
      List<FacetCountsCollection.FacetValue> facets = new ArrayList<FacetCountsCollection.FacetValue>();
      if (!facetRanges.containsKey(field)) {
        return facets;
      }
      for (Map.Entry<String, Integer> entry : facetRanges.get(field).counts.entrySet()) {
        facets.add(new FacetCountsCollection.FacetValue(entry.getKey(), entry.getValue()));
      }
      return facets;
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    @JsonSerialize(include = JsonSerialize.Inclusion.NON_NULL)
    public static class FacetRange {
      public Map<String, Integer> counts;
      public Object gap;
      public Object start;
      public Object end;
      public Object after;
      public Object before;
      public Object between;
    }

    public static class FacetValue {
      public String value;
      public Integer count;

      public FacetValue(String value, Integer count) {
        this.value = value;
        this.count = count;
      }
    }
  }

}
