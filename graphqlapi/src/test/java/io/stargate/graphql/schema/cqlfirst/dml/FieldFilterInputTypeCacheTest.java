package io.stargate.graphql.schema.cqlfirst.dml;

import static org.assertj.core.api.Assertions.assertThat;

import graphql.schema.GraphQLInputObjectType;
import io.stargate.db.schema.Column;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FieldFilterInputTypeCacheTest {

  @Mock private NameMapping nameMapping;
  private FieldFilterInputTypeCache fieldFilterInputTypes;
  private List<String> warnings = new ArrayList<>();

  @BeforeEach
  public void setup() {
    FieldInputTypeCache fieldInputTypes = new FieldInputTypeCache(nameMapping, warnings);
    fieldFilterInputTypes = new FieldFilterInputTypeCache(fieldInputTypes, nameMapping);
  }

  /**
   * The goal of this test is to ensure that our naming rules don't generate colliding GraphQL type
   * names for different CQL types. FilterInput types are particularly sensitive to that because we
   * generate custom types not only for CQL maps, but also for sets and lists.
   *
   * <p>TODO we might have to update this test when tuples get added
   */
  @Test
  @DisplayName("Should generate unique type names")
  public void collisionsTest() {
    // This example would fail if we didn't add "Entry" at the beginning of entry types
    Column.ColumnType cqlType1 =
        Column.Type.List.of(Column.Type.Map.of(Column.Type.Int, Column.Type.Text));
    Column.ColumnType cqlType2 =
        Column.Type.Map.of(Column.Type.List.of(Column.Type.Int), Column.Type.Text);

    GraphQLInputObjectType type1 = (GraphQLInputObjectType) fieldFilterInputTypes.get(cqlType1);
    GraphQLInputObjectType type2 = (GraphQLInputObjectType) fieldFilterInputTypes.get(cqlType2);

    assertThat(type1.getName()).isNotEqualTo(type2.getName());
  }
}
