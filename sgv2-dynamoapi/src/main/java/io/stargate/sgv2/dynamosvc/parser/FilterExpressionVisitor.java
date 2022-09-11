package io.stargate.sgv2.dynamosvc.parser;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.google.common.base.Preconditions;
import io.stargate.sgv2.dynamosvc.dynamo.DataMapper;
import java.util.*;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * FilterExpressionVisitor traverses the Abstract Syntax Tree (AST) built by ANTLR and evaluates the
 * filter expression result.
 *
 * <p>A FilterExpression
 * (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.FilterExpression)
 * determines which items within Query results should be returned to the user. It is applied after a
 * Query finishes, but before the results are returned. A FilterExpression can consist of a
 * combination of brackets, ANDs, ORs, etc. For each FilterExpression, this visitor class traverses
 * the corresponding AST, evaluates the expression and returns a boolean value indicating whether
 * the given item satisfies the expression.
 */
public class FilterExpressionVisitor extends ExpressionBaseVisitor<Object> {
  private final Map<String, String> nameMap;
  private final Map<String, AttributeValue> compareValueMap;
  private Map<String, AttributeValue> item;

  public FilterExpressionVisitor(
      Map<String, String> nameMap, Map<String, AttributeValue> compareValueMap) {
    Objects.requireNonNull(nameMap);
    Objects.requireNonNull(compareValueMap);
    this.nameMap = nameMap;
    this.compareValueMap = compareValueMap;
  }

  /**
   * This visitor walks through all conditions, and returns True if the given item satisfies the
   * conditions and False otherwise.
   */
  public boolean isMatch(ParseTree tree, Map<String, AttributeValue> item) {
    this.item = item;
    return (Boolean) this.visit(tree);
  }

  @Override
  public Object visitSingleExpr(ExpressionParser.SingleExprContext ctx) {
    return this.visit(ctx.term());
  }

  @Override
  public Object visitParenExpr(ExpressionParser.ParenExprContext ctx) {
    return this.visit(ctx.expr());
  }

  @Override
  public Object visitAndExpr(ExpressionParser.AndExprContext ctx) {
    if (Boolean.FALSE.equals(this.visit(ctx.left))) {
      return Boolean.FALSE;
    }
    return this.visit(ctx.right);
  }

  @Override
  public Object visitOrExpr(ExpressionParser.OrExprContext ctx) {
    if (Boolean.TRUE.equals(this.visit(ctx.left))) {
      return Boolean.TRUE;
    }
    return this.visit(ctx.right);
  }

  @Override
  public Object visitNotExpr(ExpressionParser.NotExprContext ctx) {
    return Boolean.FALSE.equals(this.visit(ctx.expr()));
  }

  @Override
  public Object visitTerm(ExpressionParser.TermContext ctx) {
    return this.visit(ctx.children.get(0));
  }

  @Override
  public Object visitOpTerm(ExpressionParser.OpTermContext ctx) {
    AttributeValue realValue = (AttributeValue) this.visit(ctx.name());
    if (realValue == null) {
      // current item does not have this attribute
      return false;
    }
    AttributeValue compareValue = (AttributeValue) this.visit(ctx.value());
    String operator = ctx.operator.getText();
    switch (operator) {
      case "=":
        return Objects.equals(realValue, compareValue);
      case "<>":
        return !Objects.equals(realValue, compareValue);
    }
    switch (operator) {
      case ">":
        return DataMapper.compareTo(realValue, compareValue) > 0;
      case ">=":
        return DataMapper.compareTo(realValue, compareValue) >= 0;
      case "<":
        return DataMapper.compareTo(realValue, compareValue) < 0;
      case "<=":
        return DataMapper.compareTo(realValue, compareValue) <= 0;
      default:
        throw new IllegalArgumentException("Operator " + operator + " is invalid");
    }
  }

  @Override
  public Object visitBetweenTerm(ExpressionParser.BetweenTermContext ctx) {
    AttributeValue lowerBound = (AttributeValue) this.visit(ctx.lower);
    AttributeValue upperBound = (AttributeValue) this.visit(ctx.upper);
    AttributeValue realValue = (AttributeValue) this.visit(ctx.name());
    if (realValue == null) {
      // current item does not have this attribute
      return false;
    }
    return DataMapper.compareTo(realValue, lowerBound) >= 0
        && DataMapper.compareTo(realValue, upperBound) <= 0;
  }

  @Override
  public Object visitBeginTerm(ExpressionParser.BeginTermContext ctx) {
    AttributeValue realValue = (AttributeValue) this.visit(ctx.name());
    AttributeValue beginValue = (AttributeValue) this.visit(ctx.value());
    if (realValue == null) {
      // current item does not have this attribute
      return false;
    }
    Preconditions.checkArgument(
        realValue.getS() != null && beginValue.getS() != null,
        "begins_with condition only applies to string type");
    return realValue.getS().startsWith(beginValue.getS());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object visitInTerm(ExpressionParser.InTermContext ctx) {
    AttributeValue realValue = (AttributeValue) this.visit(ctx.name());
    if (realValue == null) {
      // current item does not have this attribute
      return false;
    }
    List<AttributeValue> values = (List<AttributeValue>) this.visit(ctx.values());
    for (AttributeValue value : values) {
      if (DataMapper.equals(realValue, value)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Object visitSingleVal(ExpressionParser.SingleValContext ctx) {
    return new ArrayList(Arrays.asList(this.visit(ctx.value())));
  }

  @Override
  @SuppressWarnings("unchecked")
  public Object visitMultiVal(ExpressionParser.MultiValContext ctx) {
    List<AttributeValue> values = (List<AttributeValue>) this.visit(ctx.values());
    values.add((AttributeValue) this.visit(ctx.value()));
    return values;
  }

  /**
   * Given an attribute's name (could be top-level attribute or a nested attribute), retrieve its
   * corresponding value provided by database
   *
   * @param ctx
   * @return
   */
  @Override
  public Object visitName(ExpressionParser.NameContext ctx) {
    String keyword = ctx.WORD().getText();
    CharStream chars = CharStreams.fromString(keyword);
    Lexer lexer = new ProjectionLexer(chars);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    ProjectionParser parser = new ProjectionParser(tokens);
    ProjectionExpressionVisitor visitor =
        new ProjectionExpressionVisitor(nameMap, new AttributeValue().withM(item));
    ParseTree tree = parser.start();
    return visitor.find(tree);
  }

  @Override
  public Object visitValue(ExpressionParser.ValueContext ctx) {
    String placeholder = ctx.VALUE_PLACEHOLDER().getText();
    return compareValueMap.get(placeholder);
  }
}
