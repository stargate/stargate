package io.stargate.sgv2.graphql.schema.cqlfirst.dml;

import com.google.common.collect.ImmutableList;
import graphql.schema.GraphQLSchema;
import io.stargate.grpc.Values;
import io.stargate.proto.QueryOuterClass.ColumnSpec;
import io.stargate.proto.QueryOuterClass.ResultSet;
import io.stargate.proto.QueryOuterClass.Row;
import io.stargate.proto.QueryOuterClass.Value;
import io.stargate.proto.Schema.CqlKeyspaceDescribe;
import io.stargate.sgv2.graphql.schema.GraphqlTestBase;
import io.stargate.sgv2.graphql.schema.cqlfirst.SchemaFactory;
import java.util.Arrays;
import java.util.List;

public abstract class DmlTestBase extends GraphqlTestBase {

  @Override
  protected GraphQLSchema createGraphqlSchema() {
    CqlKeyspaceDescribe firstkeyspace = getCqlSchema().get(0);
    return SchemaFactory.newDmlSchema(firstkeyspace);
  }

  protected static Value listV(Object... values) {
    return Values.of(
        Arrays.stream(values)
            .map(
                o -> {
                  if (o instanceof Value) {
                    return ((Value) o);
                  } else if (o instanceof Integer) {
                    return Values.of(((Integer) o));
                  } else if (o instanceof String) {
                    return Values.of(((String) o));
                  } else {
                    throw new AssertionError("Unexpected type " + o.getClass().getName());
                  }
                })
            .toArray(Value[]::new));
  }

  protected ResultSet singleRowResultSet(List<ColumnSpec> columns, List<Value> values) {
    return ResultSet.newBuilder()
        .addAllColumns(columns)
        .addRows(Row.newBuilder().addAllValues(values))
        .build();
  }

  protected ResultSet singleRowResultSet(ColumnSpec column, Value value) {
    return singleRowResultSet(ImmutableList.of(column), ImmutableList.of(value));
  }
}
