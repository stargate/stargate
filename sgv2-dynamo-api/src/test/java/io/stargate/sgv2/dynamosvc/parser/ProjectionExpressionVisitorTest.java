package io.stargate.sgv2.dynamosvc.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;

public class ProjectionExpressionVisitorTest {
  @Test
  public void testNestedProjection() {
    Map<String, String> nameMap = new HashMap<>();
    Map<String, AttributeValue> valueMap =
        new HashMap() {
          {
            put("intCol", new AttributeValue().withN("42"));
            put(
                "listCol",
                new AttributeValue()
                    .withL(
                        new AttributeValue().withS("sub1"),
                        new AttributeValue().withS("sub2"),
                        new AttributeValue().withS("sub3")));
            put(
                "dictCol",
                new AttributeValue()
                    .withM(
                        new HashMap() {
                          {
                            put(
                                "nestedDictCol",
                                new AttributeValue()
                                    .withM(
                                        Collections.singletonMap(
                                            "text", new AttributeValue().withS("text value"))));
                            put("nestedBoolCol", new AttributeValue().withBOOL(true));
                            put(
                                "nestedLstCol",
                                new AttributeValue()
                                    .withL(
                                        new AttributeValue()
                                            .withL(
                                                new AttributeValue().withS("deep nested"),
                                                new AttributeValue()
                                                    .withM(
                                                        Collections.singletonMap(
                                                            "num",
                                                            new AttributeValue().withN("0.42"))))));
                          }
                        }));
          }
        };
    AttributeValue item = new AttributeValue().withM(valueMap);
    evaluateProjection("intCol", nameMap, item, new AttributeValue().withN("42"));
    evaluateProjection("listCol[1]", nameMap, item, new AttributeValue().withS("sub2"));
    evaluateProjection("dictCol.nestedBoolCol", nameMap, item, new AttributeValue().withBOOL(true));
    evaluateProjection(
        "dictCol.nestedDictCol.text", nameMap, item, new AttributeValue().withS("text value"));
    evaluateProjection(
        "dictCol.nestedLstCol[0][0]", nameMap, item, new AttributeValue().withS("deep nested"));
    evaluateProjection(
        "dictCol.nestedLstCol[0][1].num", nameMap, item, new AttributeValue().withN("0.42"));
  }

  private void evaluateProjection(
      String expr, Map<String, String> nameMap, AttributeValue item, AttributeValue expectedValue) {
    CharStream chars = CharStreams.fromString(expr);

    Lexer lexer = new ProjectionLexer(chars);
    CommonTokenStream tokens = new CommonTokenStream(lexer);

    ProjectionParser parser = new ProjectionParser(tokens);

    ProjectionExpressionVisitor visitor = new ProjectionExpressionVisitor(nameMap, item);
    ParseTree tree = parser.start();
    assertEquals(expectedValue, visitor.find(tree));
  }
}
