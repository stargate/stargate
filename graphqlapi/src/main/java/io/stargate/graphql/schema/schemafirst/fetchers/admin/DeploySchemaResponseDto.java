package io.stargate.graphql.schema.schemafirst.fetchers.admin;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import graphql.GraphqlErrorHelper;
import graphql.schema.DataFetchingEnvironment;
import io.stargate.graphql.schema.schemafirst.migration.MigrationQuery;
import io.stargate.graphql.schema.schemafirst.processor.ProcessingMessage;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DeploySchemaResponseDto {

  private UUID version;
  private List<ProcessingMessage> messages;
  private List<MigrationQuery> cqlChanges;

  public void setVersion(UUID version) {
    this.version = version;
  }

  public UUID getVersion() {
    return version;
  }

  public void setMessages(List<ProcessingMessage> messages) {
    this.messages = messages;
  }

  public List<Map<String, Object>> getMessages(DataFetchingEnvironment environment) {
    Stream<ProcessingMessage> stream = messages.stream();
    String category = environment.getArgument("category");
    if (category != null) {
      stream = stream.filter(m -> m.getErrorType().toString().equalsIgnoreCase(category));
    }
    return stream.map(this::formatMessage).collect(Collectors.toList());
  }

  public void setCqlChanges(List<MigrationQuery> cqlChanges) {
    this.cqlChanges = cqlChanges;
  }

  public List<String> getCqlChanges() {
    return cqlChanges.isEmpty()
        ? ImmutableList.of("No changes, the CQL schema is up to date")
        : cqlChanges.stream().map(MigrationQuery::getDescription).collect(Collectors.toList());
  }

  private Map<String, Object> formatMessage(ProcessingMessage message) {
    return ImmutableMap.of(
        "contents",
        message.getMessage(),
        "category",
        message.getErrorType().toString().toUpperCase(Locale.ENGLISH),
        "locations",
        message.getLocations().stream()
            .map(GraphqlErrorHelper::location)
            .collect(Collectors.toList()));
  }
}
