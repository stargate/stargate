package io.stargate.web.resources;

import static io.stargate.db.schema.Column.Type.*;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.db.schema.Column;
import io.stargate.web.models.TypeAdd;
import io.stargate.web.models.udt.CQLType;
import io.stargate.web.models.udt.UdtType;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

public class TypeConvertersTest {

  private static final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  public void simpleTypeAdd() throws JsonProcessingException {

    UdtType udtType1 = new UdtType();
    udtType1.setName("birthdate");
    udtType1.setBasic(CQLType.DATE);

    TypeAdd type = new TypeAdd();
    type.setFields(Collections.singletonList(udtType1));

    String typeString = objectMapper.writeValueAsString(type);

    assertThat(typeString)
        .isEqualTo(
            "{\"ifNotExists\":false,\"fields\":[{\"name\":\"birthdate\",\"basic\":\"DATE\"}]}");

    type = objectMapper.readValue(typeString, TypeAdd.class);
    assertThat(type.getFields()).isEqualTo(type.getFields());
    assertThat(type.getIfNotExists()).isFalse();
    assertThat(type.getFields().get(0).getName()).isEqualTo("birthdate");
    assertThat(type.getFields().get(0).getBasic()).isEqualTo(CQLType.DATE);
  }

  @Test
  public void complexTypeAdd() throws JsonProcessingException {
    String typeString =
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
    TypeAdd type = objectMapper.readValue(typeString, TypeAdd.class);

    List<Column> response = Converters.fromTypeAdd(type);
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
