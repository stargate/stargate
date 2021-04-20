package io.stargate.graphql.schema.cqlfirst.dml.fetchers;

import graphql.schema.DataFetchingEnvironment;
import java.util.Map;

public class TtlFromOptionsExtractor {
  static Integer getTTL(DataFetchingEnvironment environment) {
    Integer ttl = null;
    if (environment.containsArgument("options") && environment.getArgument("options") != null) {
      Map<String, Object> options = environment.getArgument("options");
      if (options.containsKey("ttl") && options.get("ttl") != null) {
        ttl = (Integer) options.get("ttl");
      }
    }
    return ttl;
  }
}
