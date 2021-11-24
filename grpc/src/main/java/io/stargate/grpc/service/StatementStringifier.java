/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.grpc.service;

import io.stargate.proto.QueryOuterClass.Value;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import stargate.Cql;
import stargate.Cql.Relation;
import stargate.Cql.Relation.Operator;
import stargate.Cql.Relation.Target;
import stargate.Cql.Relation.Target.ElementTarget;
import stargate.Cql.Relation.Target.FieldTarget;
import stargate.Cql.Relation.Target.TokenTarget;
import stargate.Cql.Relation.Target.TupleTarget;
import stargate.Cql.Select;
import stargate.Cql.Selector;
import stargate.Cql.Selector.ElementSelector;
import stargate.Cql.Selector.FieldSelector;
import stargate.Cql.Selector.FunctionCallSelector;
import stargate.Cql.Term;
import stargate.Cql.Term.FunctionCallTerm;

public class StatementStringifier {

  private final StringBuilder builder = new StringBuilder();
  private final boolean alwaysInlineValues;
  private final List<Value> values;

  public StatementStringifier(boolean alwaysInlineValues) {
    this.alwaysInlineValues = alwaysInlineValues;
    this.values = alwaysInlineValues ? Collections.emptyList() : new ArrayList<>();
  }

  public String getCql() {
    return builder.toString();
  }

  public List<Value> getValues() {
    return values;
  }

  StatementStringifier append(Cql.Statement statement) {
    if (statement.hasSelect()) {
      appendSelect(statement.getSelect());
    } else {
      throw new UnsupportedOperationException(
          "Unsupported statement type " + statement.getInnerCase().name());
    }
    return this;
  }

  private void appendSelect(Select select) {
    append("SELECT");
    if (select.getSelectorsCount() == 0) {
      append(" *");
    } else {
      appendAll(select.getSelectorsList(), " ", ",", "", this::append);
    }
    append(" FROM ");
    appendId(select.getKeyspaceName());
    append('.');
    appendId(select.getTableName());
    appendAll(select.getWhereList(), " WHERE ", " AND ", "", this::append);
  }

  private void append(Selector selector) {
    int fieldNumber = selector.getInnerCase().getNumber();
    switch (fieldNumber) {
      case Selector.COLUMN_NAME_FIELD_NUMBER:
        appendId(selector.getColumnName());
        break;
      case Selector.FIELD_FIELD_NUMBER:
        FieldSelector field = selector.getField();
        append(field.getUdt());
        append(field.getFieldName());
        break;
      case Selector.FUNCTION_CALL_FIELD_NUMBER:
        FunctionCallSelector functionCall = selector.getFunctionCall();
        if (functionCall.hasKeyspaceName()) {
          appendId(functionCall.getKeyspaceName().getValue());
          append('.');
        }
        appendId(functionCall.getFunctionName());
        append('(');
        appendAll(functionCall.getArgumentsList(), ",", this::append);
        append(')');
        break;
      case Selector.ELEMENT_FIELD_NUMBER:
        ElementSelector element = selector.getElement();
        append(element.getCollection());
        append('[');
        append(element.getIndex(), false);
        append(']');
        break;
      case Selector.VALUE_FIELD_NUMBER:
        append(selector.getValue(), false);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported selector inner field number " + fieldNumber);
    }
    if (selector.hasAlias()) {
      append(" AS ");
      append(selector.getAlias().getValue());
    }
  }

  private void append(Relation relation) {
    append(relation.getTarget());
    append(' ');
    append(relation.getOperator());
    if (relation.hasTerm()) {
      append(' ');
      append(relation.getTerm(), true);
    }
  }

  private void append(Target target) {
    int fieldNumber = target.getInnerCase().getNumber();
    switch (fieldNumber) {
      case Target.COLUMN_NAME_FIELD_NUMBER:
        appendId(target.getColumnName());
        break;
      case Target.FIELD_FIELD_NUMBER:
        FieldTarget field = target.getField();
        appendId(field.getUdt());
        append('.');
        appendId(field.getFieldName());
        break;
      case Target.ELEMENT_FIELD_NUMBER:
        ElementTarget element = target.getElement();
        appendId(element.getCollection());
        append('[');
        // Indexes can be bound, as in `IF m[?] = ?`
        // We don't need to worry about recursion, because multiple levels of indexing like
        // `m[1][2]` are not valid CQL anyway.
        append(element.getIndex(), true);
        append(']');
        break;
      case Target.TUPLE_FIELD_NUMBER:
        TupleTarget tuple = target.getTuple();
        appendAll(tuple.getColumnsList(), "(", ",", ")", this::appendId);
        break;
      case Target.TOKEN_FIELD_NUMBER:
        TokenTarget token = target.getToken();
        appendAll(token.getColumnsList(), "TOKEN(", ",", ")", this::appendId);
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported selector inner field number " + fieldNumber);
    }
  }

  private void append(Operator operator) {
    switch (operator) {
      case EQ:
        append('=');
        break;
      case LT:
        append('<');
        break;
      case LTE:
        append("<=");
        break;
      case GT:
        append('>');
        break;
      case GTE:
        append(">=");
        break;
      case NEQ:
        append("!=");
        break;
      case LIKE:
        append("LIKE");
        break;
      case IS_NOT_NULL:
        append("IS NOT NULL");
        break;
      case IN:
        append("IN");
        break;
      default:
        throw new IllegalArgumentException("Unsupported operator " + operator);
    }
  }

  private void append(Term term, boolean canBind) {
    int fieldNumber = term.getInnerCase().getNumber();
    switch (fieldNumber) {
      case Term.VALUE_FIELD_NUMBER:
        append(term.getValue(), canBind);
        break;
      case Term.FUNCTION_CALL_FIELD_NUMBER:
        FunctionCallTerm functionCall = term.getFunctionCall();
        if (functionCall.hasKeyspaceName()) {
          appendId(functionCall.getKeyspaceName().getValue());
          append('.');
        }
        appendId(functionCall.getFunctionName());
        append('(');
        // Function parameters can be bound, as in `IF col = f(?)`
        appendAll(functionCall.getArgumentsList(), ",", arg -> append(arg, true));
        append(')');
        break;
      default:
        throw new IllegalArgumentException(
            "Unsupported selector inner field number " + fieldNumber);
    }
  }

  private void append(Value value, boolean canBind) {
    if (canBind && !alwaysInlineValues) {
      appendBindMarker();
      values.add(value);
    } else {
      append(toCqlString(value));
    }
  }

  private String toCqlString(Value value) {
    int fieldNumber = value.getInnerCase().getNumber();
    switch (fieldNumber) {
      case Value.INT_FIELD_NUMBER:
        return Long.toString(value.getInt());
      case Value.STRING_FIELD_NUMBER:
        return '\'' + value.getString() + '\'';
      default:
        // TODO implement complete formatting system for all types
        throw new IllegalArgumentException(
            "Unsupported selector inner field number " + fieldNumber);
    }
  }

  private void append(String s) {
    builder.append(s);
  }

  private void append(char c) {
    builder.append(c);
  }

  /** @param id a CQL identifier in its "internal" form, meaning unquoted and in its exact case. */
  private void appendId(String id) {
    // We're not trying to generate pretty CQL, so just always quote.
    builder.append('"').append(id).append('"');
  }

  private void appendBindMarker() {
    append('?');
  }

  /** Note that prefix and suffix are only appended if the list is not empty. */
  private <T> void appendAll(
      List<T> elements, String prefix, String separator, String suffix, Consumer<T> appendOne) {
    if (!elements.isEmpty()) {
      append(prefix);
      boolean first = true;
      for (T element : elements) {
        if (first) {
          first = false;
        } else {
          append(separator);
        }
        appendOne.accept(element);
      }
      append(suffix);
    }
  }

  private <T> void appendAll(List<T> elements, String separator, Consumer<T> appendOne) {
    appendAll(elements, "", separator, "", appendOne);
  }
}
