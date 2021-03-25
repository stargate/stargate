package io.stargate.web.resources;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.stargate.web.models.UdtAdd;
import io.stargate.web.models.udt.CQLType;
import io.stargate.web.models.udt.UdtType;
import java.util.Arrays;
import org.junit.jupiter.api.Test;

public class UdtSerDeTest {

  @Test
  public void testSimpleUDT() throws JsonProcessingException {
    ObjectMapper objectMapper = new ObjectMapper();

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
}
