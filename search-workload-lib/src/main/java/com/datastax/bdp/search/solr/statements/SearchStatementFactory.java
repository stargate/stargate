package com.datastax.bdp.search.solr.statements;

import com.datastax.bdp.xml.XmlPath;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.Cql_DseSearchParser;
import org.apache.cassandra.cql3.QualifiedName;
import org.apache.cassandra.exceptions.InvalidRequestException;

public class SearchStatementFactory implements Cql_DseSearchParser.SearchStatementFactory {
  public CQLStatement.Raw createSearchIndexStatement(
      QualifiedName cf,
      boolean ifNotExists,
      List<String> columns,
      Map<String, Map<String, String>> columnOptions,
      List<String> profiles,
      Map<String, String> configOptions,
      Map<String, String> requestOptions)
      throws InvalidRequestException {
    return new CreateSearchIndexStatement(
        cf, ifNotExists, columns, columnOptions, profiles, configOptions, requestOptions);
  }

  public CQLStatement.Raw alterSearchIndexStatement(
      QualifiedName cf,
      boolean config,
      String verb,
      XmlPath path,
      String attribute,
      String value,
      String json)
      throws InvalidRequestException {
    return new AlterSearchIndexStatement(cf, config, verb, path, attribute, value, json);
  }

  public CQLStatement.Raw dropSearchIndexStatement(QualifiedName cf, Map<String, String> options)
      throws InvalidRequestException {
    return new DropSearchIndexStatement(cf, options);
  }

  public CQLStatement.Raw commitSearchIndexStatement(QualifiedName cf)
      throws InvalidRequestException {
    return new CommitSearchIndexStatement(cf);
  }

  public CQLStatement.Raw reloadSearchIndexStatement(QualifiedName cf, Map<String, String> options)
      throws InvalidRequestException {
    return new ReloadSearchIndexStatement(cf, options);
  }

  public CQLStatement.Raw rebuildSearchIndexStatement(QualifiedName cf, Map<String, String> options)
      throws InvalidRequestException {
    return new RebuildSearchIndexStatement(cf, options);
  }
}
