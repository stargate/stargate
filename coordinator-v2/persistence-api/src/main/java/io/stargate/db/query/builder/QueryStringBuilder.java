package io.stargate.db.query.builder;

import static java.lang.String.format;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.query.BindMarker;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.QualifiedSchemaEntity;
import io.stargate.db.schema.SchemaEntity;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

class QueryStringBuilder {
  private final StringBuilder internalBuilder = new StringBuilder();
  private final StringBuilder externalBuilder = new StringBuilder();
  private final BindMarker[] externalBindMarker;
  private int internalIndex = 0;

  QueryStringBuilder(int externalMarkers) {
    this.externalBindMarker = new BindMarker[externalMarkers];
  }

  private static boolean noSpaceBefore(char c) {
    switch (c) {
      case ' ':
      case ')':
      case ']':
      case ',':
        return true;
      default:
        return false;
    }
  }

  private static boolean noSpaceAfter(char c) {
    switch (c) {
      case ' ':
      case '(':
      case '[':
        return true;
      default:
        return false;
    }
  }

  private static void maybeAddSpaceBefore(String str, StringBuilder builder) {
    if (str.isEmpty()) {
      return;
    }
    boolean noSpaceAfterPrev =
        builder.length() == 0 || noSpaceAfter(builder.charAt(builder.length() - 1));
    boolean noSpaceBeforeNew = noSpaceBefore(str.charAt(0));
    if (!noSpaceAfterPrev && !noSpaceBeforeNew) {
      builder.append(' ');
    }
  }

  private static void appendWithSpaceBefore(String str, StringBuilder builder) {
    maybeAddSpaceBefore(str, builder);
    builder.append(str);
  }

  private QueryStringBuilder appendWithSpaceBefore(String str) {
    appendWithSpaceBefore(str, internalBuilder);
    appendWithSpaceBefore(str, externalBuilder);
    return this;
  }

  private QueryStringBuilder appendBoth(String str) {
    internalBuilder.append(str);
    externalBuilder.append(str);
    return this;
  }

  private QueryStringBuilder appendBoth(String... strings) {
    for (String str : strings) {
      appendBoth(str);
    }
    return this;
  }

  QueryStringBuilder append(String str) {
    return appendWithSpaceBefore(str.trim());
  }

  QueryStringBuilder appendForceNoSpace(String str) {
    return appendBoth(str.trim());
  }

  QueryStringBuilder append(QualifiedSchemaEntity entity) {
    appendWithSpaceBefore(entity.cqlKeyspace());
    appendBoth(".", entity.cqlName());
    return this;
  }

  QueryStringBuilder append(SchemaEntity entity) {
    return appendWithSpaceBefore(entity.cqlName());
  }

  QueryStringBuilder append(BindMarker marker, Value<?> value) {
    return append(marker, value, false);
  }

  QueryStringBuilder appendInValue(BindMarker marker, Value<?> value) {
    return append(marker, value, true);
  }

  private QueryStringBuilder append(BindMarker marker, Value<?> value, boolean isInValue) {
    value.setInternalIndex(internalIndex++);
    if (value.isMarker()) {
      externalBindMarker[((Value.Marker<?>) value).externalIndex()] = marker;
      return appendWithSpaceBefore("?");
    } else {
      // Internal, we prepare the value with a marker. Externally, it's a concrete value though.
      appendWithSpaceBefore("?", internalBuilder);
      Object concreteValue = value.get();
      if (isInValue) {
        // The value must be a list, but the column itself is not, so we need some special code.
        // Doubly so since IN uses parenthesis for its sub-values, while normal lists use square
        // brackets.
        Preconditions.checkArgument(
            concreteValue instanceof List,
            "On column %s, IN value must be a java List, but got %s of java type '%s'",
            marker.receiver(),
            concreteValue,
            concreteValue.getClass().getSimpleName());
        List<?> values = (List<?>) concreteValue;
        // Forcing a space, because we usually don't add one before a '(', but we want it for IN
        externalBuilder.append(" ");
        appendWithSpaceBefore("(", externalBuilder);
        ColumnType subType = marker.type().parameters().get(0);
        for (int i = 0; i < values.size(); i++) {
          if (i > 0) externalBuilder.append(", ");
          String cqlValue = valueToString("value of " + marker.receiver(), subType, values.get(i));
          appendWithSpaceBefore(cqlValue, externalBuilder);
        }
        appendWithSpaceBefore(")", externalBuilder);
      } else {
        String cqlValue = valueToString(marker.receiver(), marker.type(), concreteValue);
        appendWithSpaceBefore(cqlValue, externalBuilder);
      }
      return this;
    }
  }

  private String valueToString(String receiver, ColumnType type, Object value) {
    try {
      return type.toCQLString(value);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(
          format("Invalid value provided for '%s': %s", receiver, e.getMessage()), e);
    }
  }

  ListBuilder start() {
    return start("", ", ");
  }

  ListBuilder start(String opening) {
    return start(opening, ", ");
  }

  ListBuilder start(String opening, String separator) {
    return new ListBuilder(opening, separator, false);
  }

  ListBuilder lazyStart(String opening) {
    return lazyStart(opening, ", ");
  }

  ListBuilder lazyStart(String opening, String separator) {
    return new ListBuilder(opening, separator, true);
  }

  List<BindMarker> externalBindMarkers() {
    List<BindMarker> markers = Arrays.asList(externalBindMarker);
    Preconditions.checkState(markers.stream().noneMatch(Objects::isNull));
    return markers;
  }

  int internalBindMarkers() {
    return internalIndex;
  }

  public String internalQueryString() {
    return internalBuilder.toString();
  }

  public String externalQueryString() {
    return externalBuilder.toString();
  }

  @Override
  public String toString() {
    return format("{internal=%s, external=%s}", internalQueryString(), externalQueryString());
  }

  class ListBuilder {
    private final String opening;
    private final String separator;
    private final boolean ignoreEmpty;
    private boolean isFirst = true;

    private ListBuilder(String opening, String separator, boolean ignoreEmpty) {
      this.opening = opening;
      this.separator = separator;
      this.ignoreEmpty = ignoreEmpty;
    }

    private void beforeElement() {
      if (isFirst) {
        appendWithSpaceBefore(opening);
        isFirst = false;
      } else {
        appendWithSpaceBefore(separator);
      }
    }

    <T extends SchemaEntity> ListBuilder addAll(List<T> l) {
      return addAll(l, QueryStringBuilder.this::append);
    }

    <T> ListBuilder addAll(List<T> l, Consumer<T> onElement) {
      return addAllWithIdx(l, (e, i) -> onElement.accept(e));
    }

    <T> ListBuilder addAllWithIdx(List<T> l, BiConsumer<T, Integer> onElement) {
      for (int i = 0; i < l.size(); i++) {
        beforeElement();
        onElement.accept(l.get(i), i);
      }
      return this;
    }

    <T> ListBuilder addIfNotNull(T element, Consumer<T> onElement) {
      if (element != null) {
        beforeElement();
        onElement.accept(element);
      }
      return this;
    }

    <T> ListBuilder addIf(boolean condition, Runnable onTrue) {
      if (condition) {
        beforeElement();
        onTrue.run();
      }
      return this;
    }

    QueryStringBuilder end() {
      return end("");
    }

    QueryStringBuilder end(String ending) {
      if (isFirst) {
        if (!ignoreEmpty) {
          appendWithSpaceBefore(opening).appendWithSpaceBefore(ending);
        }
      } else {
        appendWithSpaceBefore(ending);
      }
      return QueryStringBuilder.this;
    }
  }
}
