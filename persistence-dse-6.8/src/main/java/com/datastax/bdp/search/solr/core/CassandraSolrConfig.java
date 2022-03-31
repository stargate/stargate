/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core;

import com.datastax.bdp.search.solr.core.types.CassandraSolrTypeMapper;
import com.datastax.bdp.search.solr.core.types.CassandraSolrTypeMapper.TypeMappingVersion;
import com.datastax.bdp.search.solr.core.types.V1TypeMapper;
import com.datastax.bdp.search.solr.core.types.V2TypeMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathConstants;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientWarn;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.FixedDocumentsWriterPerThreadPool;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.DirectoryFactory;
import org.apache.solr.core.PluginInfo;
import org.apache.solr.core.SolrConfig;
import org.apache.solr.core.SolrResourceLoader;
import org.apache.solr.handler.RealTimeGetHandler;
import org.apache.solr.handler.ReplicationHandler;
import org.apache.solr.handler.SQLHandler;
import org.apache.solr.handler.StreamHandler;
import org.apache.solr.handler.component.RealTimeGetComponent;
import org.apache.solr.index.TieredMergePolicyFactory;
import org.apache.solr.search.join.BlockJoinChildQParserPlugin;
import org.apache.solr.search.join.BlockJoinParentQParserPlugin;
import org.apache.solr.search.join.GraphQParserPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class CassandraSolrConfig extends SolrConfig {
  public static final int DEFAULT_MAX_MERGE_AT_ONCE = 10; // TODO-SEARCH
  public static final String DSE_TYPE_MAPPING_VERSION = "dseTypeMappingVersion";
  public static final String DSE_KEY_BLOOM_FILTER = "dseUseUniqueKeyBloomFilter";
  public static final String DSE_TYPE_MAPPING_FORCE = "force";
  public static final String DSE_ALLOW_TOKENIZED_UNIQUE_KEY = "dseAllowTokenizedUniqueKey";

  private static final HashMap<
          CassandraSolrTypeMapper.TypeMappingVersion, CassandraSolrTypeMapper.Factory>
      mappers =
          new HashMap<
              CassandraSolrTypeMapper.TypeMappingVersion, CassandraSolrTypeMapper.Factory>() {
            {
              put(CassandraSolrTypeMapper.TypeMappingVersion.VERSION_1, new V1TypeMapper.Factory());
              put(CassandraSolrTypeMapper.TypeMappingVersion.VERSION_2, new V2TypeMapper.Factory());
            }
          };

  private static final Set<String> unsupportedRequestHandlers =
      ImmutableSet.of(
          ReplicationHandler.class.getSimpleName(),
          RealTimeGetHandler.class.getSimpleName(),
          StreamHandler.class.getSimpleName(),
          SQLHandler.class.getSimpleName(),
          "DataImportHandler");

  private static final Set<String> unsupportedQParsers =
      ImmutableSet.of(
          BlockJoinParentQParserPlugin.NAME,
          BlockJoinChildQParserPlugin.NAME,
          GraphQParserPlugin.NAME);

  private static final Set<String> unsupportedSearchComponents =
      ImmutableSet.of(RealTimeGetComponent.class.getSimpleName());

  private static final Logger logger = LoggerFactory.getLogger(CassandraSolrConfig.class);

  private volatile CassandraSolrTypeMapper typeMapper;
  private volatile boolean useUniqueKeyBloomFilter;
  private volatile boolean allowTokenizedUniqueKey;
  private volatile NamedList<Object> updateHandlerDefaults;

  public static CassandraSolrTypeMapper.Factory getTypeMapperByVersion(String version) {
    return CassandraSolrConfig.mappers.get(
        CassandraSolrTypeMapper.TypeMappingVersion.lookup(version));
  }

  public CassandraSolrConfig(
      SolrResourceLoader loader, String configName, InputSource is, String coreName)
      throws ParserConfigurationException, IOException, SAXException {
    super(loader, configName, is);
    // loadDsePlugins();
    loadDseTypeMapper();
    loadDseBloomFilterConfig();
    loadTokenizedUniqueKeyValidationOption();
    initIndexerThreadPool();
    // initThreadPoolFactory();
    initLiveIndexing();
    // initSolrMetricsEventListener(coreName);
    // initRamBufferAllocationCallback(coreName);
  }

  // TODO-SEARCH
  // @Override
  // public List<PluginInfo> readPluginInfos(String tag, boolean requireName, boolean requireClass)

  // TODO-SEARCH
  // @Override
  // public UpdateHandlerInfo getUpdateHandlerInfo()

  @Override
  public Set<String> getUnsupportedRequestHandlers() {
    return unsupportedRequestHandlers;
  }

  @Override
  public Set<String> getUnsupportedQParsers() {
    return unsupportedQParsers;
  }

  public void setTypeMapper(TypeMappingVersion version) {
    if (mappers.containsKey(version)) {
      typeMapper = mappers.get(version).make(false);
    } else {
      throw new IllegalStateException("Missed type mapping version: " + version.getVersion());
    }
  }

  public CassandraSolrTypeMapper getTypeMapper() {
    return typeMapper;
  }

  public boolean shouldUseUniqueKeyBloomFilter() {
    return useUniqueKeyBloomFilter;
  }

  public boolean isTokenizedUniqueKeyAllowed() {
    return allowTokenizedUniqueKey;
  }

  private void runChecks() {
    checkHandleSelect();
    checkSchemaSetup();
    checkUpdateLog();
    checkDirectoryFactory();
    checkQueryDocumentCache();
    checkMaxDocs();
    checkRequestHandlers();
    checkSearchComponents();
    checkFilterCache();
    checkDataDir();
    checkramBufferSizeMB();
    this.indexConfig.useUninvertingReaderInDeleteByQueryWrapper = false;
  }

  private void checkDataDir() {
    Node schemaFactory = getNode("dataDir", false);
    if (schemaFactory != null) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Solr config: 'dataDir' is not supported. Remove it from the solrconfig.");
    }
  }

  private void checkSchemaSetup() {
    Node schemaFactory = getNode("schemaFactory", false);
    if (schemaFactory != null) {
      Node schemaClass = schemaFactory.getAttributes().getNamedItem("class");
      if (schemaClass.getNodeValue().equals("ManagedIndexSchemaFactory")) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Solr config: Managed Schema Definition is not allowed as schema modification operations are not supported. Remove it from the solrconfig.");
      }
    }
  }

  private void checkHandleSelect() {
    if (!this.isHandleSelect()) {
      throw new SolrException(
          SolrException.ErrorCode.SERVER_ERROR,
          "Solr config requestDispatcher must have handleSelect=true.");
    }
  }

  private void checkUpdateLog() {

    Node updateLogNode = getNode("//updateHandler/updateLog", false);
    if (updateLogNode != null) {
      Node forceAttribute = updateLogNode.getAttributes().getNamedItem("force");
      if ((forceAttribute == null) || !forceAttribute.getNodeValue().equals("true")) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Solr config: 'updateLog' is not supported. Remove it from the solrconfig.");
      }
    }
  }

  private void checkDirectoryFactory() {
    PluginInfo info = getPluginInfo(DirectoryFactory.class.getName());

    if (info != null) {
      DirectoryFactory directoryFactory =
          getResourceLoader().newInstance(info.className, DirectoryFactory.class);

      if (!directoryFactory.isPersistent()) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Solr config: directoryFactory must be persistent.");
      }
    }
  }

  private void checkRequestHandlers() {
    NodeList requestHandlers = (NodeList) evaluate("requestHandler", XPathConstants.NODESET);
    if (requestHandlers != null && requestHandlers.getLength() > 0) {
      for (int i = 0; i < requestHandlers.getLength(); ++i) {
        Node requestHandler = requestHandlers.item(i);
        String handlerClass = requestHandler.getAttributes().getNamedItem("class").getTextContent();
        if (unsupportedRequestHandlers.stream().anyMatch(handlerClass::endsWith)) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              String.format(
                  "Solr config: DSE Search does not support request handler [%s]. Remove it from the solrconfig.",
                  handlerClass));
        }
      }
    }
  }

  private void checkSearchComponents() {
    NodeList searchComponents = (NodeList) evaluate("searchComponent", XPathConstants.NODESET);
    if (searchComponents != null && searchComponents.getLength() > 0) {
      for (int i = 0; i < searchComponents.getLength(); ++i) {
        Node searchComponent = searchComponents.item(i);
        String componentClass =
            searchComponent.getAttributes().getNamedItem("class").getTextContent();
        if (unsupportedSearchComponents.stream().anyMatch(componentClass::endsWith)) {
          throw new SolrException(
              SolrException.ErrorCode.SERVER_ERROR,
              String.format(
                  "Solr config: DSE Search does not support search component [%s]. Remove it from the solrconfig.",
                  componentClass));
        }
      }
    }
  }

  private void checkQueryDocumentCache() {
    checkNodeAndWarn(
        "//query/documentCache",
        "Solr config: Query document cache is configured but will not be used");
  }

  private void checkMaxDocs() {
    checkNodeAndWarn(
        "//updateHandler/autoSoftCommit/maxDocs",
        "Solr config: autoSoftCommit.maxDocs is not supported");
    checkNodeAndWarn(
        "//updateHandler/autoCommit/maxDocs", "Solr config: autoCommit.maxDocs is not supported");
  }

  private void checkramBufferSizeMB() {
    checkNodeAndWarn(
        "//indexConfig/ramBufferSizeMB", "Solr config: ramBufferSizeMB is not supported");
  }

  private void checkFilterCache() {
    Node filterCacheNode = getNode("//query/filterCache", false);
    if (filterCacheNode != null) {
      Node classAttribute = filterCacheNode.getAttributes().getNamedItem("class");
      Node lowWaterMarkAttribute = filterCacheNode.getAttributes().getNamedItem("lowWaterMarkMB");
      Node highWaterMarkAttribute = filterCacheNode.getAttributes().getNamedItem("highWaterMarkMB");
      if (!classAttribute.getNodeValue().equals("solr.SolrFilterCache")
          && ((lowWaterMarkAttribute != null) || (highWaterMarkAttribute != null))) {
        throw new SolrException(
            SolrException.ErrorCode.SERVER_ERROR,
            "Solr config: lowWaterMarkMB and highWaterMarkMB attributes are only supported with solr.SolrFilterCache");
      }
    }
  }

  private void checkNodeAndWarn(String nodeXpath, String warning) {
    Node node = getNode(nodeXpath, false);

    if (node != null) {
      logger.warn(warning);
      ClientWarn.instance.warn(warning);
    }
  }

  /*private void loadDsePlugins() throws IOException
  {
      loadPluginInfo(new SolrPluginInfo(DSEUpdateRequestProcessorChain.class, "dseUpdateRequestProcessorChain", PluginOpts.NOOP));
      loadPluginInfo(new SolrPluginInfo(FieldInputTransformer.class, "fieldInputTransformer", PluginOpts.NOOP));
      loadPluginInfo(new SolrPluginInfo(FieldOutputTransformer.class, "fieldOutputTransformer", PluginOpts.NOOP));
  }*/

  private void loadDseTypeMapper() {
    NodeList nodes = (NodeList) evaluate(DSE_TYPE_MAPPING_VERSION, XPathConstants.NODESET);
    if (nodes != null && nodes.getLength() > 0) {
      Node typeMappingNode = nodes.item(0);
      Node forcedNode = typeMappingNode.getAttributes().getNamedItem(DSE_TYPE_MAPPING_FORCE);
      String versionValue = CassandraSolrTypeMapper.TypeMappingVersion.VERSION_1.getVersion();
      boolean forcedValue = false;
      if (typeMappingNode.getTextContent() != null) {
        versionValue = typeMappingNode.getTextContent();
      }
      if (forcedNode != null) {
        forcedValue = Boolean.parseBoolean(forcedNode.getTextContent());
      }

      CassandraSolrTypeMapper.TypeMappingVersion candidate =
          CassandraSolrTypeMapper.TypeMappingVersion.lookup(versionValue);
      if (mappers.containsKey(candidate)) {
        typeMapper = mappers.get(candidate).make(forcedValue);
      } else {
        throw new IllegalStateException("Missed type mapping version: " + candidate.getVersion());
      }
    } else {
      typeMapper = mappers.get(CassandraSolrTypeMapper.TypeMappingVersion.VERSION_1).make(false);
    }
  }

  private void loadDseBloomFilterConfig() {
    NodeList nodes = (NodeList) evaluate(DSE_KEY_BLOOM_FILTER, XPathConstants.NODESET);
    if (nodes != null && nodes.getLength() > 0) {
      Node bloomFilterConfigNode = nodes.item(0);
      useUniqueKeyBloomFilter = Boolean.parseBoolean(bloomFilterConfigNode.getTextContent());
    } else {
      useUniqueKeyBloomFilter = true;
    }
  }

  private void loadTokenizedUniqueKeyValidationOption() {
    NodeList nodes = (NodeList) evaluate(DSE_ALLOW_TOKENIZED_UNIQUE_KEY, XPathConstants.NODESET);
    if (nodes != null && nodes.getLength() > 0) {
      Node configNode = nodes.item(0);
      allowTokenizedUniqueKey = Boolean.parseBoolean(configNode.getTextContent());
    } else {
      allowTokenizedUniqueKey = false;
    }
  }

  private void initIndexerThreadPool() {
    this.indexerThreadPool =
        new FixedDocumentsWriterPerThreadPool(DatabaseDescriptor.getTPCCores());
    this.indexConfig.setIndexerThreadPool(this.indexerThreadPool);
  }

  /*private void initThreadPoolFactory()
  {
      this.indexConfig.setThreadPools(Suppliers.memoize(() -> DseSearchModule.getContainerPlugin().getThreadPools())::get);
  }*/

  private void initLiveIndexing() {
    this.indexConfig.rtDWPTNarrowDeletes = true;
  }

  /*private void initSolrMetricsEventListener(String coreName)
  {
      SolrMetricsEventListener listener = new SolrMetricsEventListener(coreName);
      this.indexConfig.setWriteEventListener(listener);
      this.indexConfig.setReaderEventListener(listener);
  }

  private void initRamBufferAllocationCallback(String coreName)
  {
      this.indexConfig.setRamBufferAllocationCallback(
              Suppliers.memoize(
                      () -> CassandraCoreContainer.getInstance()
                              .getSolrSecondaryIndex(coreName, true)
                              .getRamBufferAllocationsTracker())::get
      );
  }*/

  @SuppressWarnings("unchecked")
  private void tuneMergeScheduler(String tag, List<PluginInfo> plugins) {
    // Override Lucene's default configuration for the concurrent merge scheduler
    // if no user provided configuration exists
    if (tag.equalsIgnoreCase("indexConfig/mergeScheduler")) {
      for (PluginInfo plugin : plugins) {
        // Check for a merge scheduler plugin, if one was specified don't mess with the config
        if (plugin.type.equals("mergeScheduler")) {
          logger.info(
              "Using \"mergeScheduler\" from provided solrconfig.xml with \"maxThreadCount\" = {} and \"maxMergeCount\" = {}...",
              plugin.initArgs.get("maxThreadCount"),
              plugin.initArgs.get("maxMergeCount"));
          return;
        }
      }

      // This scales the number of active merges along with the number of TPC threads:
      int maxThreadCount = Math.max(1, DatabaseDescriptor.getTPCCores() / 2);

      // This deviates from the Lucene 6 default in that it may create a deeper queue of pending
      // merges:
      int maxMergeCount =
          Math.max(
              Math.max(maxThreadCount * 2, DatabaseDescriptor.getNumTokens() * 8),
              maxThreadCount + 5);

      logger.info(
          "Configuring \"mergeScheduler\" with \"maxThreadCount\" = {} and \"maxMergeCount\" = {} "
              + "i.e. max(max(<maxThreadCount * 2>, <num_tokens * 8>), <maxThreadCount + 5>)...",
          maxThreadCount,
          maxMergeCount);

      Map<String, String> mergeSchedulerAttributes =
          ImmutableMap.of("class", ConcurrentMergeScheduler.class.getCanonicalName());

      NamedList mergeSchedulerInitArgs =
          new NamedList(
              ImmutableMap.of(
                  "maxThreadCount", maxThreadCount,
                  "maxMergeCount", maxMergeCount));
      PluginInfo mergeScheduler =
          new PluginInfo(
              "mergeScheduler",
              mergeSchedulerAttributes,
              mergeSchedulerInitArgs,
              ImmutableList.of());
      plugins.add(mergeScheduler);
    }
  }

  @SuppressWarnings("unchecked")
  private void tuneMergePolicy(String tag, List<PluginInfo> plugins) {
    // Override Lucene's default merge policy configuration if no user provided configuration exists
    if (tag.equalsIgnoreCase("indexConfig/mergePolicyFactory")) {
      for (PluginInfo plugin : plugins) {
        // Check for a merge policy factory, if one was specified don't mess with the config
        if (plugin.type.equals("mergePolicyFactory")) {
          logger.info(
              "Using \"mergePolicyFactory\" from provided solrconfig.xml with args: {}",
              plugin.initArgs);
          return;
        }
      }

      Map<String, String> mergePolicyFactoryAttributes =
          ImmutableMap.of("class", TieredMergePolicyFactory.class.getCanonicalName());

      NamedList mergePolicyInitArgs =
          new NamedList(ImmutableMap.of("maxMergeAtOnce", DEFAULT_MAX_MERGE_AT_ONCE));
      PluginInfo mergeScheduler =
          new PluginInfo(
              "mergePolicyFactory",
              mergePolicyFactoryAttributes,
              mergePolicyInitArgs,
              ImmutableList.of());
      plugins.add(mergeScheduler);

      logger.info("Configured \"mergePolicyFactory\" with args: {}", mergePolicyInitArgs);
    }
  }
}
