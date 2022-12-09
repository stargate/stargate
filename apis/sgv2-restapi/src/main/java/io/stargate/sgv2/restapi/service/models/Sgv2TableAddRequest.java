package io.stargate.sgv2.restapi.service.models;

import java.util.Collections;
import java.util.List;
import javax.validation.constraints.NotBlank;
import org.eclipse.microprofile.openapi.annotations.media.Schema;

public record Sgv2TableAddRequest(
    @Schema(required = true, description = "The name of the table to add.")
        @NotBlank(message = "TableAdd.name must be provided")
        String name,
    @Schema(
            required = true,
            description =
                "The primary key definition of the table, consisting of partition and clustering keys.")
        Sgv2Table.PrimaryKey primaryKey,
    @Schema(
            required = true,
            description = "Definition of columns that belong to the table to be added.")
        List<Sgv2ColumnDefinition> columnDefinitions,
    @Schema(
            description =
                "Determines whether to create a new table if a table with the same name exists. Attempting to create an existing table returns an error unless this option is true.")
        boolean ifNotExists,
    @Schema(description = "The set of table options to apply to the table when creating.")
        Sgv2Table.TableOptions tableOptions) {
  public Sgv2TableAddRequest(
      String name,
      Sgv2Table.PrimaryKey primaryKey,
      List<Sgv2ColumnDefinition> columnDefinitions,
      boolean ifNotExists,
      Sgv2Table.TableOptions tableOptions) {
    this.name = name;
    this.primaryKey = primaryKey;
    this.columnDefinitions = columnDefinitions;
    this.ifNotExists = ifNotExists;
    this.tableOptions = (tableOptions == null) ? new Sgv2Table.TableOptions() : tableOptions;
  }
  //  Sgv2Table.TableOptions tableOptions = new Sgv2Table.TableOptions();

  // // // Convenience access

  public List<Sgv2Table.ClusteringExpression> findClusteringExpressions() {
    if (tableOptions != null) {
      List<Sgv2Table.ClusteringExpression> clustering = tableOptions.getClusteringExpression();
      if (clustering != null) {
        return clustering;
      }
    }
    return Collections.emptyList();
  }
}
