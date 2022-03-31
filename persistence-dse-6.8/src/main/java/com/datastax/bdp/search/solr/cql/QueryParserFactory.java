/*
 * Copyright DataStax, Inc.
 *
 * Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.cql;

import java.util.regex.Pattern;

/** QueryParser factory to build the right instance depending on the input query. */
public class QueryParserFactory {
  private static final Pattern JSON_PATTERN = Pattern.compile("^\\{(?!\\!).+");

  public QueryParser build(String query) {
    if (isJSON(query)) {
      return new SimpleJSONQueryParser();
    } else {
      return new QQueryParser();
    }
  }

  private boolean isJSON(String query) {
    return JSON_PATTERN.matcher(query.trim()).matches();
  }
}
