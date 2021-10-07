package io.stargate.web.restapi.resources;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

import io.dropwizard.testing.junit5.DropwizardExtensionsSupport;
import io.dropwizard.testing.junit5.ResourceExtension;
import io.stargate.auth.AuthorizationService;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.ImmutableColumn;
import io.stargate.db.schema.Table;
import io.stargate.web.restapi.dao.RestDB;
import io.stargate.web.restapi.dao.RestDBFactory;
import io.stargate.web.restapi.models.ColumnDefinition;
import io.stargate.web.restapi.resources.v1.ColumnResource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.ws.rs.core.GenericType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(DropwizardExtensionsSupport.class)
class ColumnResourceTest {

  private static final RestDBFactory db = mock(RestDBFactory.class);

  private static final ResourceExtension resource =
      ResourceExtension.builder().addResource(new ColumnResource(db)).build();

  @AfterEach
  void resetMocks() {
    reset(db);
  }

  @Test
  void listAllColumnsSuccess() throws Exception {
    AuthorizationService authorizationService = mock(AuthorizationService.class);
    Table table = mock(Table.class);
    Column column1 = ImmutableColumn.create("c1", Column.Kind.Static, Column.Type.Text);
    Column column2 = ImmutableColumn.create("c2", Column.Kind.Regular, Column.Type.Int);
    List<Column> columns = Arrays.asList(column1, column2);

    RestDB restDB = mock(RestDB.class);
    when(db.getRestDBForToken("token", Collections.emptyMap())).thenReturn(restDB);
    when(restDB.getTable("keySpaceName", "tableName")).thenReturn(table);
    when(table.columns()).thenReturn(columns);

    List<ColumnDefinition> columnDefinitions =
        resource
            .target("/v1/keyspaces/keySpaceName/tables/tableName/columns")
            .request()
            .header("X-Cassandra-Token", "token")
            .get(new GenericType<List<ColumnDefinition>>() {});

    assertThat(columnDefinitions)
        .usingRecursiveComparison()
        .isEqualTo(
            Arrays.asList(
                new ColumnDefinition("c1", "text", true),
                new ColumnDefinition("c2", "int", false)));
  }
}
