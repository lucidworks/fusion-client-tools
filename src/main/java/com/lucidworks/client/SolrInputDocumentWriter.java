package com.lucidworks.client;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

/**
 * Defines a generic writer of Solr data.
 * <p>
 * Implementations of this class may write directly to Solr, or to any other underlying
 * store than can handle SolrInputDocuments.
 */
public interface SolrInputDocumentWriter {

  /**
   * Write a collection of documents to an underlying datastore.
   *
   * @param shard            shard id (ignored when using solr cloud)
   * @param inputDocumentMap map of document ids to {@code SolrInputDocument}s
   */
  void add(int shard, Map<String, SolrInputDocument> inputDocumentMap) throws SolrServerException, IOException;

  /**
   * Delete a list of documents from an underlying datastore (optional operation).
   *
   * @param shard shard id (ignored when using solr cloud)
   */
  void deleteById(int shard, List<String> idsToDelete) throws SolrServerException, IOException;

  /**
   * Has the same behavior as {@link SolrClient#deleteByQuery(String)} (optional operation).
   *
   * @param deleteQuery delete query to be executed
   */
  void deleteByQuery(String deleteQuery) throws SolrServerException, IOException;

  /**
   * Close any open resources being used by this writer.
   */
  void close() throws SolrServerException, IOException;

}