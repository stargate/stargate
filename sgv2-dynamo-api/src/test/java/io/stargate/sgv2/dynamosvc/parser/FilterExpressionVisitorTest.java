package io.stargate.sgv2.dynamosvc.parser;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;

public class FilterExpressionVisitorTest {
  @Test
  public void testSimpleOperatorExpression() {
    Map<String, String> nameMap = Collections.singletonMap("#N", "name");
    Map<String, AttributeValue> compareValueMap =
        new HashMap() {
          {
            put(":val1", new AttributeValue().withN("42"));
            put(":val2", new AttributeValue().withN("4.2"));
            put(":val3", new AttributeValue().withS("alice"));
          }
        };
    evaluateFilterExpression(
        "#N = :val3",
        nameMap,
        compareValueMap,
        Arrays.asList(Collections.singletonMap("name", new AttributeValue("alice"))),
        Collections.singletonList(true));
    evaluateFilterExpression(
        "name < :val3",
        nameMap,
        compareValueMap,
        Arrays.asList(Collections.singletonMap("name", new AttributeValue("alice"))),
        Collections.singletonList(false));
    evaluateFilterExpression(
        "name>= :val3",
        nameMap,
        compareValueMap,
        Arrays.asList(
            Collections.singletonMap("name", new AttributeValue("alice")),
            Collections.singletonMap("name", new AttributeValue("bob"))),
        Arrays.asList(true, true));
    evaluateFilterExpression(
        "#N <>:val3",
        nameMap,
        compareValueMap,
        Arrays.asList(
            Collections.singletonMap("name", new AttributeValue("alice")),
            Collections.singletonMap("name", new AttributeValue("ALICE"))),
        Arrays.asList(false, true));
    evaluateFilterExpression(
        "(num <= :val2 )",
        nameMap,
        compareValueMap,
        Arrays.asList(
            Collections.singletonMap("num", new AttributeValue().withN("4.2")),
            Collections.singletonMap("num", new AttributeValue().withN("4.20001")),
            Collections.singletonMap("num", new AttributeValue().withN("4")),
            Collections.singletonMap("name", new AttributeValue("alice"))),
        Arrays.asList(true, false, true, false));
  }

  @Test
  public void testAndOrNotExpression() {
    Map<String, String> nameMap = Collections.singletonMap("#N", "name");
    Map<String, AttributeValue> compareValueMap =
        new HashMap() {
          {
            put(":val1", new AttributeValue().withN("42"));
            put(":val2", new AttributeValue().withN("4.2"));
            put(":val3", new AttributeValue().withS("alice"));
          }
        };

    evaluateFilterExpression(
        "#N = :val3 And age > :val1",
        nameMap,
        compareValueMap,
        Arrays.asList(
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("age", new AttributeValue().withN("43"));
              }
            },
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("age", new AttributeValue().withN("42"));
              }
            }),
        Arrays.asList(true, false));

    evaluateFilterExpression(
        "#N = :val3 OR age > :val1",
        nameMap,
        compareValueMap,
        Arrays.asList(
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("age", new AttributeValue().withN("43"));
              }
            },
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("age", new AttributeValue().withN("42"));
              }
            }),
        Arrays.asList(true, true));
    evaluateFilterExpression(
        "age > :val1 OR NOT #N = :val3",
        nameMap,
        compareValueMap,
        Arrays.asList(
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("age", new AttributeValue().withN("43"));
              }
            },
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("age", new AttributeValue().withN("42"));
              }
            }),
        Arrays.asList(true, false));
  }

  @Test
  public void testBetweenExpression() {
    Map<String, AttributeValue> compareValueMap =
        new HashMap() {
          {
            put(":lowNum", new AttributeValue().withN("0"));
            put(":highNum", new AttributeValue().withN("0.5"));
            put(":lowS", new AttributeValue().withS("alex"));
            put(":highS", new AttributeValue().withS("bob"));
          }
        };

    evaluateFilterExpression(
        "num BETWEEN :lowNum and :highNum",
        Collections.emptyMap(),
        compareValueMap,
        Arrays.asList(
            Collections.singletonMap("num", new AttributeValue().withN("1")),
            Collections.singletonMap("num", new AttributeValue().withN("0")),
            Collections.singletonMap("num", new AttributeValue().withN("0.0")),
            Collections.singletonMap("num", new AttributeValue().withN("0.5")),
            Collections.singletonMap("num", new AttributeValue().withN("-1"))),
        Arrays.asList(false, true, true, true, false));

    evaluateFilterExpression(
        "word BETWEEN :lowS   and :highS",
        Collections.emptyMap(),
        compareValueMap,
        Arrays.asList(
            Collections.singletonMap("word", new AttributeValue("aa")),
            Collections.singletonMap("word", new AttributeValue("alex")),
            Collections.singletonMap("word", new AttributeValue("bob")),
            Collections.singletonMap("word", new AttributeValue("alice")),
            Collections.singletonMap("word", new AttributeValue("bobby"))),
        Arrays.asList(false, true, true, true, false));
  }

  @Test
  public void testBeginsWithExpression() {
    Map<String, AttributeValue> compareValueMap =
        new HashMap() {
          {
            put(":s", new AttributeValue().withS("won"));
          }
        };
    evaluateFilterExpression(
        " BEGINS_WITH (word,:s)",
        Collections.emptyMap(),
        compareValueMap,
        Arrays.asList(
            Collections.singletonMap("word", new AttributeValue("win")),
            Collections.singletonMap("word", new AttributeValue("won")),
            Collections.singletonMap("word", new AttributeValue("wonder")),
            Collections.singletonMap("word", new AttributeValue("wonderful")),
            Collections.singletonMap("word", new AttributeValue("wo"))),
        Arrays.asList(false, true, true, true, false));
  }

  @Test
  public void testInExpression() {
    Map<String, AttributeValue> compareValueMap =
        new HashMap() {
          {
            put(":s1", new AttributeValue().withS("apple"));
            put(":s2", new AttributeValue().withN("123"));
            put(":s3", new AttributeValue().withBOOL(false));
          }
        };
    evaluateFilterExpression(
        "word IN (:s1, :s2,:s3)",
        Collections.emptyMap(),
        compareValueMap,
        Arrays.asList(
            Collections.singletonMap("word", new AttributeValue("appl")),
            Collections.singletonMap("word", new AttributeValue("apple")),
            Collections.singletonMap("word", new AttributeValue().withN("123")),
            Collections.singletonMap("word", new AttributeValue().withBOOL(false)),
            Collections.singletonMap("word", new AttributeValue("false"))),
        Arrays.asList(false, true, true, true, false));
  }

  @Test
  public void testNestedExpression() {
    Map<String, AttributeValue> compareValueMap =
        new HashMap() {
          {
            put(":val1", new AttributeValue().withN("42"));
            put(":val2", new AttributeValue().withN("4.2"));
            put(":val3", new AttributeValue().withS("alice"));
          }
        };

    evaluateFilterExpression(
        "num1 = :val1 or num2=:val2 And name <> :val3",
        Collections.emptyMap(),
        compareValueMap,
        Arrays.asList(
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("num2", new AttributeValue().withN("0"));
                put("num1", new AttributeValue().withN("42"));
              }
            },
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("num2", new AttributeValue().withN("0"));
                put("num1", new AttributeValue().withN("0"));
              }
            }),
        Arrays.asList(true, false));

    evaluateFilterExpression(
        "(num1 = :val1 or num2=:val2) and name <> :val3",
        Collections.emptyMap(),
        compareValueMap,
        Arrays.asList(
            new HashMap() {
              {
                put("name", new AttributeValue("alice"));
                put("num2", new AttributeValue().withN("0"));
                put("num1", new AttributeValue().withN("42"));
              }
            },
            new HashMap() {
              {
                put("name", new AttributeValue("bob"));
                put("num2", new AttributeValue().withN("4.2"));
                put("num1", new AttributeValue().withN("0"));
              }
            },
            new HashMap() {
              {
                put("name", new AttributeValue("bob"));
                put("num2", new AttributeValue().withN("0"));
                put("num1", new AttributeValue().withN("42"));
              }
            }),
        Arrays.asList(false, true, true));
  }

  /**
   * Helper function to evaluate the given FilterExpression with different item results. It then
   * compares them to desired results (boolean values indicating whether these items should be
   * retained or filtered out).
   *
   * @param expr A FilterExpression in plain string format, e.g. "#num BETWEEN :lowNum and :highNum"
   * @param nameMap A hashmap storing the mapping from name placeholders to real names. Name
   *     placeholders are string starting with a hashtag.
   * @param compareValueMap A hashmap storing the mapping from value placeholders to real values.
   *     Value placeholders are string starting with a colon sign.
   * @param items A list of items to test.
   * @param shouldRetain A list of expected match results.
   */
  void evaluateFilterExpression(
      String expr,
      Map<String, String> nameMap,
      Map<String, AttributeValue> compareValueMap,
      List<Map<String, AttributeValue>> items,
      List<Boolean> shouldRetain) {
    CharStream chars = CharStreams.fromString(expr);

    Lexer lexer = new ExpressionLexer(chars);
    CommonTokenStream tokens = new CommonTokenStream(lexer);

    ExpressionParser parser = new ExpressionParser(tokens);

    FilterExpressionVisitor visitor = new FilterExpressionVisitor(nameMap, compareValueMap);
    ParseTree tree = parser.expr();
    for (int i = 0; i < items.size(); i++) {
      Map<String, AttributeValue> item = items.get(i);
      assertEquals(shouldRetain.get(i), visitor.isMatch(tree, item));
    }
  }
}
