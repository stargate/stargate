package io.stargate.it.http.restapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;
import io.stargate.it.driver.CqlSessionExtension;
import io.stargate.it.driver.CqlSessionSpec;
import io.stargate.it.http.RestUtils;
import io.stargate.web.restapi.models.IndexAdd;
import io.stargate.web.restapi.models.SuccessResponse;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import net.jcip.annotations.NotThreadSafe;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Integration tests for REST API v2 that test Storage-Attached Index (SAI) dependant functionality.
 * Separate from other tests to allow exclusion when testing backends that test(s) would not work
 * with.
 */
@NotThreadSafe
@ExtendWith(CqlSessionExtension.class)
@CqlSessionSpec()
public class RestApiv2SAITest extends RestApiTestBase {
  @Test
  public void indexCreateCustom(CqlSession session) throws IOException {
    // TODO remove this when we figure out how to enable SAI indexes in Cassandra 4
    assumeThat(backendSupportsSAI())
        .as("Disabled because it is currently not possible to enable SAI indexes on this backend")
        .isTrue();

    createTestKeyspace(keyspaceName);
    tableName = "tbl_createtable_" + System.currentTimeMillis();
    createTestTable(
        tableName,
        Arrays.asList("id text", "firstName text", "lastName text", "email list<text>"),
        Collections.singletonList("id"),
        null);

    IndexAdd indexAdd = new IndexAdd();
    String indexType = "org.apache.cassandra.index.sasi.SASIIndex";
    indexAdd.setColumn("lastName");
    indexAdd.setName("test_custom_idx");
    indexAdd.setType(indexType);
    indexAdd.setIfNotExists(false);
    indexAdd.setKind(null);

    Map<String, String> options = new HashMap<>();
    options.put("mode", "CONTAINS");
    indexAdd.setOptions(options);

    String body =
        RestUtils.post(
            authToken,
            String.format(
                "%s/v2/schemas/keyspaces/%s/tables/%s/indexes",
                restUrlBase, keyspaceName, tableName),
            objectMapper.writeValueAsString(indexAdd),
            HttpStatus.SC_CREATED);
    SuccessResponse successResponse = objectMapper.readValue(body, SuccessResponse.class);
    assertThat(successResponse.getSuccess()).isTrue();

    Collection<Row> rows = session.execute("SELECT * FROM system_schema.indexes;").all();
    Optional<Row> row =
        rows.stream().filter(i -> "test_custom_idx".equals(i.getString("index_name"))).findFirst();
    Map<String, String> optionsReturned = row.get().getMap("options", String.class, String.class);

    assertThat(optionsReturned.get("class_name")).isEqualTo(indexType);
    assertThat(optionsReturned.get("target")).isEqualTo("\"lastName\"");
    assertThat(optionsReturned.get("mode")).isEqualTo("CONTAINS");
  }
}
