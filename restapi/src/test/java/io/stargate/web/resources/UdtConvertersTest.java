package io.stargate.web.resources;

import static io.stargate.db.schema.Column.Type.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.db.schema.Column;
import io.stargate.web.models.UdtAdd;
import io.stargate.web.models.udt.CQLType;
import io.stargate.web.models.udt.UdtType;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

public class UdtConvertersTest {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void simpleUdtAdd() throws JsonProcessingException {

    UdtType udtType1 = new UdtType();
    udtType1.setName("birthdate");
    udtType1.setBasic(CQLType.DATE);

    UdtAdd udt = new UdtAdd();
    udt.setFields(Arrays.asList(udtType1));

    String udtString = objectMapper.writeValueAsString(udt);

    assertThat(udtString)
        .isEqualTo(
            "{\"ifNotExists\":false,\"fields\":[{\"name\":\"birthdate\",\"basic\":\"DATE\"}]}");

    udt = objectMapper.readValue(udtString, UdtAdd.class);
    assertThat(udt.getFields()).isEqualTo(udt.getFields());
    assertThat(udt.getIfNotExists()).isFalse();
    assertThat(udt.getFields().get(0).getName()).isEqualTo("birthdate");
    assertThat(udt.getFields().get(0).getBasic()).isEqualTo(CQLType.DATE);
  }

  @Test
  public void complexUdtAdd() throws JsonProcessingException {
    String udtString =
        "{ "
            + " \"fields\": [{ "
            + "   \"name\": \"col1\", "
            + "   \"basic\": \"TUPLE\", "
            + "   \"info\": { "
            + "    \"typeParams\": [{ "
            + "     \"basic\": \"DATE\" "
            + "    }, { "
            + "     \"basic\": \"INT\" "
            + "    }], "
            + "    \"frozen\": true "
            + "   } "
            + "  }, "
            + "  { "
            + "   \"name\": \"col2\", "
            + "   \"basic\": \"LIST\", "
            + "   \"info\": { "
            + "    \"typeParams\": [{ "
            + "     \"basic\": \"UUID\" "
            + "    }], "
            + "    \"frozen\": true "
            + "   } "
            + "  }, "
            + "  { "
            + "   \"name\": \"col3\", "
            + "   \"basic\": \"MAP\", "
            + "   \"info\": { "
            + "    \"typeParams\": [{ "
            + "      \"basic\": \"TEXT\" "
            + "     }, "
            + "     { "
            + "      \"basic\": \"INT\" "
            + "     } "
            + "    ] "
            + "   } "
            + "  } "
            + " ] "
            + "}";
    UdtAdd udt = objectMapper.readValue(udtString, UdtAdd.class);

    List<Column> response = Converters.fromUdtAdd(udt);
    assertThat(response.size()).isEqualTo(3);

    for (Column column : response) {
      switch (column.name()) {
        case "col1":
          assertThat(column.type()).isEqualTo(Tuple.of(Date, Int));
          break;
        case "col2":
          assertThat(column.type()).isEqualTo(List.of(Uuid).frozen(true));
          break;
        case "col3":
          assertThat(column.type()).isEqualTo(Map.of(Text, Int));
          break;
      }
    }
  }
}
