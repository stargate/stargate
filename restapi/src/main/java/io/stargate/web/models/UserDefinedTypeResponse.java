package io.stargate.web.models;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.swagger.annotations.ApiModelProperty;
import java.util.List;

/**
 * Represent a user defined type at api level.
 *
 * @author Cedrick LUNVEN (@clunven)
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class UserDefinedTypeResponse {

  /** Unique identifier. */
  private final String name;

  /** Keyspace. */
  private final String keyspace;

  /** List of columns. */
  private final List<ColumnDefinitionUserDefinedType> columnDefinitions;

  @JsonCreator
  public UserDefinedTypeResponse(
      @JsonProperty("name") final String name,
      @JsonProperty("keyspace") final String keyspace,
      @JsonProperty("columnDefinitions")
          final List<ColumnDefinitionUserDefinedType> columnDefinitions) {
    this.name = name;
    this.keyspace = keyspace;
    this.columnDefinitions = columnDefinitions;
  }

  @ApiModelProperty(value = "The name of the user defined type.")
  public String getName() {
    return name;
  }

  @ApiModelProperty(value = "Name of the keyspace the user defined type belongs.")
  public String getKeyspace() {
    return keyspace;
  }

  @ApiModelProperty(value = "Definition of columns within the user defined type.")
  public List<ColumnDefinitionUserDefinedType> getColumnDefinitions() {
    return columnDefinitions;
  }
}
