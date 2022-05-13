package io.stargate.sgv2.dynamosvc.parser;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import java.util.Map;
import org.antlr.v4.runtime.tree.ParseTree;

/**
 * ProjectionExpressionVisitor traverses the Abstract Syntax Tree (AST) built by ANTLR and evaluates
 * the projection result.
 *
 * <p>A projection expression
 * (https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.Attributes.html) is
 * a string that identifies one or more attributes to retrieve from the table. It is an optional
 * request parameter in GetItemAPI
 * (https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_GetItem.html#DDB-GetItem-request-ProjectionExpression)
 * and QueryAPI
 * (https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-FilterExpression).
 * Although not documented by AWS, it could also be used in QueryAPI as part of the FilterExpression
 * (https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-FilterExpression).
 *
 * <p>For example, if the query returns a row (an item) with columns (Description, RelatedItems,
 * Pictures), where Description is a string attribute, RelatedItems is a list attribute, and
 * Pictures is a map attribute. If ProjectionExpression is not given, then DynamoDB will return all
 * the three columns. Users could use ProjectionExpression to control what attributes or
 * sub-attributes they want. A valid expression would be "Description, RelatedItems[0].Price,
 * Pictures.FrontView[1]". For simplicity, we treat each of them (comma-separated) as a single
 * ProjectionExpression, and build ASTs for them. For each ProjectionExpression, this visitor class
 * traverses the corresponding AST, evaluates the expression and fetches the projected value from
 * given item.
 */
public class ProjectionExpressionVisitor extends ProjectionBaseVisitor<Object> {

  private final Map<String, String> nameMap;
  private AttributeValue item;

  public ProjectionExpressionVisitor(Map<String, String> nameMap, AttributeValue item) {
    this.nameMap = nameMap;
    this.item = item;
  }

  public AttributeValue find(ParseTree tree) {
    return (AttributeValue) this.visit(tree);
  }

  @Override
  public Object visitMapAttr(ProjectionParser.MapAttrContext ctx) {
    AttributeValue parent = (AttributeValue) this.visit(ctx.outer);
    item = parent;
    return this.visit(ctx.inner);
  }

  @Override
  public Object visitTopAttr(ProjectionParser.TopAttrContext ctx) {
    String name = (String) this.visit(ctx.children.get(0));
    return item.getM().get(name);
  }

  @Override
  public Object visitArrayAttr(ProjectionParser.ArrayAttrContext ctx) {
    AttributeValue parent = (AttributeValue) this.visit(ctx.attr());
    int offset = Integer.parseInt(ctx.OFFSET().getText());
    return parent.getL().get(offset);
  }

  @Override
  public Object visitDirectTerm(ProjectionParser.DirectTermContext ctx) {
    return ctx.WORD().getText();
  }

  @Override
  public Object visitIndirectTerm(ProjectionParser.IndirectTermContext ctx) {
    return nameMap.get(ctx.NAME_PLACEHOLDER().getText());
  }
}
