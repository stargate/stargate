package io.stargate.sgv2.dynamosvc.dynamo;

import static com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel.DynamoDBAttributeType.valueOf;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperFieldModel;
import com.amazonaws.services.dynamodbv2.model.*;
import io.stargate.proto.QueryOuterClass;
import io.stargate.sgv2.common.cql.builder.Column;
import io.stargate.sgv2.common.cql.builder.ImmutableColumn;
import io.stargate.sgv2.common.cql.builder.QueryBuilder;
import io.stargate.sgv2.common.grpc.StargateBridgeClient;
import io.stargate.sgv2.dynamosvc.models.PrimaryKey;
import java.io.IOException;
import java.util.*;

public class TableProxy extends Proxy {
  public CreateTableResult createTable(
      CreateTableRequest createTableRequest, StargateBridgeClient bridge) throws IOException {
    final String tableName = createTableRequest.getTableName();
    List<Column> columns = new ArrayList<>();
    final PrimaryKey primaryKey = getPrimaryKey(createTableRequest.getKeySchema());
    for (AttributeDefinition columnDef : createTableRequest.getAttributeDefinitions()) {
      final String columnName = columnDef.getAttributeName();
      final DynamoDBMapperFieldModel.DynamoDBAttributeType type =
          valueOf(columnDef.getAttributeType());
      ImmutableColumn.Builder column =
          ImmutableColumn.builder().name(columnName).type(DataTypeMapper.fromDynamo(type));
      if (columnName.equals(primaryKey.getPartitionKey())) {
        column.kind(Column.Kind.PARTITION_KEY);
      } else if (columnName.equals(primaryKey.getClusteringKey())) {
        column.kind(Column.Kind.CLUSTERING);
      }
      columns.add(column.build());
    }

    QueryOuterClass.Query query =
        new QueryBuilder().create().table(KEYSPACE_NAME, tableName).column(columns).build();

    bridge.executeQuery(query);

    TableDescription newTableDesc =
        this.getTableDescription(
            tableName,
            createTableRequest.getAttributeDefinitions(),
            createTableRequest.getKeySchema());
    return (new CreateTableResult()).withTableDescription(newTableDesc);
  }

  private TableDescription getTableDescription(
      String tableName,
      Collection<AttributeDefinition> attributeDefinitions,
      Collection<KeySchemaElement> keySchema) {
    TableDescription tableDescription =
        (new TableDescription())
            .withTableName(tableName)
            .withAttributeDefinitions(attributeDefinitions)
            .withKeySchema(keySchema)
            .withTableStatus(TableStatus.ACTIVE)
            .withCreationDateTime(new Date())
            .withTableArn(tableName);

    return tableDescription;
  }
}
