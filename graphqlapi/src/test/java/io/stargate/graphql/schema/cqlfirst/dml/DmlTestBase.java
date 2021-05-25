package io.stargate.graphql.schema.cqlfirst.dml;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import graphql.schema.GraphQLSchema;
import io.stargate.db.datastore.ArrayListBackedRow;
import io.stargate.db.datastore.Row;
import io.stargate.db.schema.Column;
import io.stargate.graphql.schema.GraphQlTestBase;
import io.stargate.graphql.schema.cqlfirst.SchemaFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class DmlTestBase extends GraphQlTestBase {

  @Override
  protected GraphQLSchema createGraphQlSchema() {
    return SchemaFactory.newDmlSchema(getCQLSchema().keyspaces().iterator().next());
  }

  /** Creates a basic row suitable for faking result sets. */
  protected Row createRow(List<Column> columns, Map<String, Object> data) {
    List<ByteBuffer> values = new ArrayList<>(columns.size());
    for (Column column : columns) {
      Object v = data.get(column.name());
      values.add(v == null ? null : column.type().codec().encode(v, ProtocolVersion.DEFAULT));
    }
    return new ArrayListBackedRow(columns, values, ProtocolVersion.DEFAULT);
  }
}
