package com.lucidworks.client;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.MetricName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.LBHttpSolrServer;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrInputField;

import org.apache.solr.common.util.DateUtil;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Writes updates (new documents and deletes) to Fusion.
 */
public class FusionDocumentWriter {

  private Log log = LogFactory.getLog(FusionDocumentWriter.class);

  public static MetricName metricName(Class<?> producerClass, String metric, String indexerName) {
    return new MetricName("hbaseindexer", producerClass.getSimpleName(), metric, indexerName);
  }

  private Meter indexAddMeter;
  private Meter indexDeleteMeter;
  private Meter solrAddErrorMeter;
  private Meter solrDeleteErrorMeter;
  private Meter documentAddErrorMeter;
  private Meter documentDeleteErrorMeter;

  protected FusionPipelineClient pipelineClient;
  private String solrProxies;
  protected SolrServer solrProxy;

  public FusionDocumentWriter(String indexName, Map<String, String> connectionParams) {

    String fusionEndpoint = connectionParams.get("fusion.pipeline");
    if (fusionEndpoint == null)
      throw new IllegalStateException("The 'fusion.pipeline' parameter is required when using Lucidworks Fusion!");

    String fusionSolrProxy = connectionParams.get("fusion.solrproxy");
    if (fusionSolrProxy == null)
      throw new IllegalStateException("The 'fusion.solrproxy' parameter is required when using Lucidworks Fusion!");

    String fusionUser = connectionParams.get("fusion.user");
    String fusionPass = connectionParams.get("fusion.pass");
    String fusionRealm = connectionParams.get("fusion.realm");

    log.info("Connecting to Fusion pipeline "+fusionEndpoint+" as "+fusionUser+", realm="+fusionRealm);
    try {
      pipelineClient = new FusionPipelineClient(fusionEndpoint, fusionUser, fusionPass, fusionRealm);
    } catch (Exception exc) {
      log.error("Failed to create FusionPipelineClient for "+fusionEndpoint+" due to: "+exc);
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(exc);
      }
    }

    try {
      solrProxy = new LBHttpSolrServer(pipelineClient.getHttpClient(), fusionSolrProxy.split(","));
    } catch (Exception exc) {
      log.error("Failed to create LBHttpSolrServer for "+fusionSolrProxy+" due to: "+exc);
      if (exc instanceof RuntimeException) {
        throw (RuntimeException)exc;
      } else {
        throw new RuntimeException(exc);
      }
    }

    solrProxies = fusionSolrProxy; // just used for logging below

    indexAddMeter = Metrics.newMeter(metricName(getClass(), "Index adds", indexName), "Documents added to Solr index",
      TimeUnit.SECONDS);
    indexDeleteMeter = Metrics.newMeter(metricName(getClass(), "Index deletes", indexName),
      "Documents deleted from Solr index", TimeUnit.SECONDS);
    solrAddErrorMeter = Metrics.newMeter(metricName(getClass(), "Solr add errors", indexName),
      "Documents not added to Solr due to Solr errors", TimeUnit.SECONDS);
    solrDeleteErrorMeter = Metrics.newMeter(metricName(getClass(), "Solr delete errors", indexName),
      "Documents not deleted from Solr due to Solr errors", TimeUnit.SECONDS);
    documentAddErrorMeter = Metrics.newMeter(metricName(getClass(), "Document add errors", indexName),
      "Documents not added to Solr due to document errors", TimeUnit.SECONDS);
    documentDeleteErrorMeter = Metrics.newMeter(metricName(getClass(), "Document delete errors", indexName),
      "Documents not deleted from Solr due to document errors", TimeUnit.SECONDS);

    log.info("Fusion document writer initialized successfully for " + fusionEndpoint);
  }

  public void add(int shard, Map<String, SolrInputDocument> inputDocumentMap) throws SolrServerException, IOException {
    Collection<SolrInputDocument> inputDocuments = inputDocumentMap.values();

    // shs: Processing added to handle atomic updates to documents that were being passed in via the inputDocumentMap.
    try {
      inputDocuments = addAtomicUpdateDocuments(inputDocuments);
      // Catch the exception so that any remaining documents in the collection will be able to be processed and submitted
      // to the fusion indexing pipeline.
    } catch (Exception exc) {
      throw new RuntimeException(exc);
    }

    // shs: Only process the inputDocuments collection if there are still documents remaining in the collection after
    //      the atomic updates have been processed.
    if (inputDocuments != null && !inputDocuments.isEmpty()) {
      List<Map<String, Object>> fusionDocs = null;
      try {
        fusionDocs = toJsonDocs(null, inputDocuments, 0);
      } catch (Exception exc) {
        throw new RuntimeException(exc);
      }

      try {
        pipelineClient.postBatchToPipeline(fusionDocs);
        indexAddMeter.mark(inputDocuments.size());
      } catch (Exception e) {
        retryAddsIndividually(fusionDocs);
      }
    }
  }

  /**
   * shs: New method added for handling Atomic Updates.
   * @param inputDocuments
   * @return inputDocuments (modified: the atomic update documents have been removed from the incoming parameter and
   *         only non-atomic update documents remain in that collection). The null value will be returned if all
   *         documents from the list of SolrInputDocuments have been removed.
   * @throws SolrServerException
   * @throws IOException
   */
  protected Collection<SolrInputDocument> addAtomicUpdateDocuments(Collection<SolrInputDocument> inputDocuments)
          throws SolrServerException, IOException  {
    // Visit each document in the collection and determine if it is a document that is an atomic update request. If it
    // is then add it to the atomicUpdateDocs and remove it from inputDocuments. The documents that are determined to be
    // atomic update type documents will be sent directly to the solr proxy for indexing, thereby using the Fusion Solr
    // service for ONLY those documents that have atomic updates in them. The rest of the documents, without atomic
    // updates as contained in the collection inputDocuments will be returned for processing by the Fusion pipeline.
    Collection<SolrInputDocument> atomicUpdateDocuments = new ArrayList<SolrInputDocument>();

    // This for loop utilizes an iterator which makes it possible to remove items from the inputDocuments collection
    // without getting a concurrent update exception.
    for (Iterator<SolrInputDocument> docIterator = inputDocuments.iterator(); docIterator.hasNext();) {
      SolrInputDocument doc = docIterator.next();
      for (SolrInputField sif : doc.values()) {
        Object val = sif.getValue();
        // If the type of the field just retrieved from the document is a Map object, then this is an atomic update
        // document and it should be added to the atomicUpdateDocs and also removed from the inputDocuments.
        if (val instanceof Map) {
          atomicUpdateDocuments.add(doc);
          docIterator.remove();
          // If we have one field that is a Map, then the document is an atomic update document and it is not
          // necessary to check the remaining fields in the document.
          break;
        }
      }
    }

    // The following processing is necessary IFF there are documents in the atomicUpdateDocuments collection, which
    // means that we have documents needing an atomic update.
    if (atomicUpdateDocuments != null && !atomicUpdateDocuments.isEmpty()) {
      // For Fusion document submission, the SolrInputDocument must be converted to JSON; however, since these
      // documents are going directly to Solr and are NOT being submitted to a Fusion indexing pipeline, they do not
      // need to be converted to JSON.
      try {
        // Submit atomic update documents to Solr at this point.
        solrProxy.add(atomicUpdateDocuments);
        indexAddMeter.mark(atomicUpdateDocuments.size());
      } catch (Exception e) {
        retrySolrAddsIndividually(atomicUpdateDocuments);
      }
    }
    // Finally, return any documents remaining in the inputDocuments collection for processing through the Fusion
    // indexing pipeline.
    return inputDocuments.isEmpty() ? null : inputDocuments;
  }

  /**
   * shs: This method was modified to add the parent and docCount parameters. These were added to support the recursion
   *      needed to flatten (aka denormalize) the parent/child document to any nesting level the input document may have
   *      been nested to.
   * @param parentSolrDoc   This is the parent document of the list of childSolrDocs. It is in SolrInputDocument format.
   *                        This parameter will be 'null' when first called. When this method is called recursively,
   *                        it will not be null.
   * @param childSolrDocs   This is the list documents that are the children (nested) documents of parentSolrDoc.
   * @param docCount        This is the number of child documents for a parent. It is used as the child ID
   * @return                The list documents to be sent to Fusion for indexing formatted as JSON documents.
   * @throws Exception
   */
  protected List<Map<String,Object>> toJsonDocs(SolrInputDocument parentSolrDoc, Collection<SolrInputDocument> childSolrDocs, int docCount) throws Exception {
    List<Map<String,Object>> list = new ArrayList<Map<String,Object>>(childSolrDocs.size());
    for (SolrInputDocument childSolrDoc : childSolrDocs) {
      list.addAll(toJson(parentSolrDoc, childSolrDoc, docCount));
    }
    return list;
  }

  /**
   * shs: This method was modified from its original to add the input parameters 'parent' and 'docCount'. This was done
   *      to enable recursion to be used to find all parent/child relationships to any level.
   * @param parent    The parent document for the child document being passed in. Parent may be null if the child being
   *                  passed in is a member of the initial documents submitted.
   * @param child     This is the child document. This document will be examined to determine if there are nested
   *                  child documents in it.
   * @param docCount  This is the number of child documents for a parent. It is used as the child ID
   * @return          The list of JSON formatted documents, denormalized (aka. flattened) and ready to be send to the
   *                  Fusion indexing pipeline endpoint.
   * @throws Exception
   */
  protected List<Map<String,Object>> toJson(SolrInputDocument parent, SolrInputDocument child, int docCount) throws Exception {
    List<Map<String,Object>> docs = new ArrayList<Map<String, Object>>();
    List<SolrInputDocument> childDocs = child.getChildDocuments();
    if (childDocs != null && !childDocs.isEmpty()) {
      //for (SolrInputDocument child : childDocs) {
        // docs.add(doc2json(doc, child, docCount++));
        docs.addAll(toJsonDocs(child, childDocs, docCount++));
      //}
    } else {
      // docs.add(doc2json(null, doc, docCount++));
      // I'm not certain the increment should be on the docCount here...
      docs.add(doc2json(parent, child, docCount++));
    }
    return docs;
  }

  /**
   * shs: This method was modified from its original to add the input parameters 'parent' and 'docCount'. This was done
   *      to enable recursion to be used to find all parent/child relationships to any level. The method will merge the
   *      fields in the parent document into the child document and will then convert that merged document into JSON
   *      format and return that JSON document to the caller.
   * @param parent    The parent document for the child document being passed in. Parent may be null if the child being
   *                  passed in is a member of the initial documents submitted.
   * @param child     This is the child document. It will have the parent's fields merged into it.
   * @param docCount  This is a count of the number of documents that have been added in this processing.
   * @return          The merged parent and child documents as a JSON formatted document, in a format acceptable to
   *                  Fusion.
   */
  protected Map<String,Object> doc2json(SolrInputDocument parent, SolrInputDocument child, int docCount) {
    Map<String,Object> json = new HashMap<String,Object>();
    String docId = (String) child.getFieldValue("id");
    if (docId == null) {
      if (parent != null) {
        String parentId = (String)parent.getFieldValue("id");
        docId = parentId+"-"+ docCount;
      }
      if (docId == null)
        throw new IllegalStateException("Couldn't resolve the id for document: "+ child);
    }
    json.put("id", docId);

    List fields = new ArrayList();
    if (parent != null) {
      // have a parent doc ... flatten by adding all parent doc fields to this doc with prefix _p_
      for (String f : parent.getFieldNames()) {
        if ("id".equals(f)) {
          fields.add(mapField("_p_id_s", null /* field name prefix */, parent.getField("id").getFirstValue()));
        } else {
          appendField(child, f, "_p_", fields);
        }
      }
    }

    for (String f : child.getFieldNames()) {
      if (!"id".equals(f)) { // id already added
        appendField(child, f, null, fields);
      }
    }

    // keep track of the time we saw this doc on the hbase side
    fields.add(mapField("_hbasets_tdt", null, DateUtil.getThreadLocalDateFormat().format(new Date())));

    json.put("fields", fields);

    return json;
  }

  protected void appendField(SolrInputDocument doc, String f, String pfx, List fields) {
    SolrInputField field = doc.getField(f);
    int vc = field.getValueCount();
    if (vc <= 0)
      return; // no values to add for this field

    if (vc == 1) {
      Map<String,Object> fieldMap = mapField(f, pfx, field.getFirstValue());
      if (fieldMap != null)
        fields.add(fieldMap);
    } else {
      for (Object val : field.getValues()) {
        Map<String,Object> fieldMap = mapField(f, pfx, val);
        if (fieldMap != null)
          fields.add(fieldMap);
      }
    }
  }

  protected Map<String,Object> mapField(String f, String pfx, Object val) {
    Map<String,Object> fieldMap = new HashMap<String, Object>(10);
    String fieldName = (pfx != null) ? pfx+f : f;
    fieldMap.put("name", fieldName);
    fieldMap.put("value", val);
    return fieldMap;
  }

  private void retryAddsIndividually(List<Map<String,Object>> docs) throws SolrServerException, IOException {
    for (Map<String,Object> next : docs) {
      try {
        pipelineClient.postBatchToPipeline(Collections.singletonList(next));
        indexAddMeter.mark();
      } catch (Exception e) {
        log.error("Failed to index document ["+next.get("id")+"] due to: " + e + "; doc: " + next);
        documentAddErrorMeter.mark();
      }
    }
  }

  /**
   * shs: This method was added to handle documents that are submitted as atomic updates. It is the final option to
   *      get documents into the index if the previous indexing attempt failed when sending the entire colleciton of
   *      documents to the Solr proxy. In this case, each document in the collection of documents will be submitted to
   *      the Solr proxy, one document at a time.
   * @param docs
   * @throws SolrServerException
   * @throws IOException
   */
  private void retrySolrAddsIndividually(Collection<SolrInputDocument> docs) throws SolrServerException, IOException {
    for (SolrInputDocument nextDoc : docs) {
      try {
        solrProxy.add(nextDoc);
        indexAddMeter.mark();
      } catch (Exception e) {
        log.error("Failed to index atomic update document ["+nextDoc.get("id")+"] due to: " + e + "; doc: " + nextDoc);
        // shs: Not sure if this should remain "documentAddErrorMeter" instead of being changed to solrAddErrorMeter.
        //      For now, I will leave it as a solrAddErrorMeter.
        solrAddErrorMeter.mark();
      }
    }
  }

  public void deleteById(int shard, List<String> idsToDelete) throws SolrServerException, IOException {

    int len = 15;
    String listLogInfo = (idsToDelete.size() > len) ?
      (idsToDelete.subList(0,len).toString()+" + "+(idsToDelete.size()-len)+" more ...") : idsToDelete.toString();
    log.info("Sending a deleteById '"+idsToDelete+"' to Solr(s) at: "+solrProxies);

    try {
      solrProxy.deleteById(idsToDelete, 500);
      indexDeleteMeter.mark(idsToDelete.size());
    } catch (Exception e) {
      log.error("Delete docs by id failed due to: "+e+"; ids: "+idsToDelete+". Retry deleting individually by id.");
      retryDeletesIndividually(idsToDelete);
    }
  }

  private void retryDeletesIndividually(List<String> idsToDelete) throws SolrServerException, IOException {
    for (String idToDelete : idsToDelete) {
      try {
        solrProxy.deleteById(idToDelete, 500);
        indexDeleteMeter.mark();
      } catch (SolrException e) {
        log.error("Failed to delete document with ID "+idToDelete+" due to: "+e+". Retry deleting by query as '"+idToDelete+"|||*");
        // shs: Changes here were made for Zendesk ticket 4186: Hbase row delete not deleting row in Fusion.
        //      The problem reported is that when deleting by a parent document ID, after the denormalization of the
        //      documents to be indexed, there are no documents with the parent document's ID (which is the HBase row
        //      number). If the retryDeletesIndividually() failes, then a call will be made to deleteByQuery() with
        //      the query string set to the value of idToDelete with the string "|||*" appended: <idToDelete>|||*. This
        //      should enable Solr to delete all documents that originated from the specified HBase row ID, but which have
        //      had their ID modified to include not only the original parent document's id, but have also had the
        //      three pipe characters "|||" and any number of characters there after appended. Thus, a delete by query
        //      where the query is "<HBaseRowId>|||*" should delete all child documents that came from that parent
        //      documents row in the HBase table.
        String deleteQuery = idToDelete + "|||*";
        try {
          deleteByQuery(deleteQuery);
        } catch (SolrServerException ssExc) {
          log.error("Delete doc by query after 'deleteById' failed due to: "+ssExc+", the delete query string was: 'deleteQuery'");
          documentDeleteErrorMeter.mark();
        }
      }
    }
  }

  public void deleteByQuery(String deleteQuery) throws SolrServerException, IOException {
    log.info("Sending a deleteByQuery '"+deleteQuery+"' to Solr(s) at: "+solrProxies);
    try {
      solrProxy.deleteByQuery(deleteQuery, 500);
    } catch (Exception e) {
      log.error("Failed to execute deleteByQuery: "+deleteQuery+" due to: "+e);
    }
  }

  public void close() {
    log.info("shutting down pipeline client and solr proxy");
    try {
      pipelineClient.shutdown();
    } catch (Exception ignore){}

    try {
      solrProxy.shutdown();
    } catch (Exception ignore){}
  }

}
