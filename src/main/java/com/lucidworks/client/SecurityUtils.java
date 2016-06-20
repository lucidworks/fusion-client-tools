/*
 * LucidWorks, Inc. Developer License Agreement
 *
 */
package com.lucidworks.client;

import org.apache.solr.client.solrj.impl.HttpClientUtil;
import org.apache.solr.client.solrj.impl.Krb5HttpClientConfigurer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SecurityUtils {
  public static final String LWWW_JAAS_FILE = "lww.jaas.file";
  public static final String LWWW_JAAS_APPNAME = "lww.jaas.appname";

  private static Log log = LogFactory.getLog(SecurityUtils.class);

  public static void setSecurityConfig() {
    final String jassFile = System.getProperty(LWWW_JAAS_FILE);
    if (jassFile != null) {
      log.info("Using kerberized Solr.");
      System.setProperty("java.security.auth.login.config", jassFile);
      final String appname = System.getProperty(LWWW_JAAS_APPNAME, "Client");
      System.setProperty("solr.kerberos.jaas.appname", appname);
      HttpClientUtil.setConfigurer(new Krb5HttpClientConfigurer());
    }
  }
}