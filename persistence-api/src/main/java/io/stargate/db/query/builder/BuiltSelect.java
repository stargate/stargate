package io.stargate.db.query.builder;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import io.stargate.db.query.AsyncQueryExecutor;
import io.stargate.db.query.BindMarker;
import io.stargate.db.query.BoundSelect;
import io.stargate.db.query.Query;
import io.stargate.db.query.QueryType;
import io.stargate.db.query.RowsImpacted;
import io.stargate.db.query.TypedValue;
import io.stargate.db.query.TypedValue.Codec;
import io.stargate.db.schema.Column;
import io.stargate.db.schema.Column.ColumnType;
import io.stargate.db.schema.Column.Type;
import io.stargate.db.schema.Table;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.stargate.utils.MD5Digest;

public class BuiltSelect extends BuiltQuery<BuiltSelect.Bound> {
  private final Table table;
  private final String externalQueryString;
  private final String internalQueryString;
  private final Set<Column> selectedColumns;
  private final List<Value<?>> internalWhereValues;
  private final List<BindMarker> internalBindMarkers;
  private final List<BuiltCondition> whereClause;
  private final @Nullable Value<Long> limit;

  protected BuiltSelect(
      Table table,
      Codec codec,
      AsyncQueryExecutor executor,
      QueryStringBuilder builder,
      Set<Column> selectedColumns,
      List<Value<?>> internalWhereValues,
      List<BindMarker> internalBindMarkers,
      List<BuiltCondition> whereClause,
      @Nullable Value<Long> limit) {
    this(
        table,
        codec,
        null,
        executor,
        builder.externalQueryString(),
        builder.externalBindMarkers(),
        builder.internalQueryString(),
        selectedColumns,
        internalWhereValues,
        internalBindMarkers,
        whereClause,
        limit);
    int internalBoundValuesCount = internalWhereValues.size() + (limit == null ? 0 : 1);
    Preconditions.checkArgument(
        builder.internalBindMarkers() == internalBoundValuesCount,
        "Provided %s values, but the builder has seen %s values",
        internalWhereValues.size(),
        builder.internalBindMarkers());
  }

  private BuiltSelect(
      Table table,
      Codec codec,
      @Nullable MD5Digest preparedId,
      AsyncQueryExecutor executor,
      String externalQueryString,
      List<BindMarker> unboundMarkers,
      String internalQueryString,
      Set<Column> selectedColumns,
      List<Value<?>> internalWhereValues,
      List<BindMarker> internalBindMarkers,
      List<BuiltCondition> whereClause,
      @Nullable Value<Long> limit) {
    super(QueryType.SELECT, codec, preparedId, executor, unboundMarkers);
    this.table = table;
    this.internalQueryString = internalQueryString;
    this.externalQueryString = externalQueryString;
    this.selectedColumns = selectedColumns;
    this.internalWhereValues = internalWhereValues;
    this.internalBindMarkers = internalBindMarkers;
    this.whereClause = whereClause;
    this.limit = limit;
  }

  public Table table() {
    return table;
  }

  public Set<Column> selectedColumns() {
    return selectedColumns;
  }

  @Override
  public String queryStringForPreparation() {
    return internalQueryString;
  }

  @Override
  protected Bound createBoundQuery(List<TypedValue> values) {
    TypedValue[] internalBoundValues = new TypedValue[internalBindMarkers.size()];
    for (int i = 0; i < internalWhereValues.size(); i++) {
      Value<?> internalValue = internalWhereValues.get(i);
      BindMarker internalMarker = internalBindMarkers.get(i);
      TypedValue v =
          convertValue(internalValue, internalMarker.receiver(), internalMarker.type(), values);
      int internalIndex = internalValue.internalIndex();
      Preconditions.checkState(internalIndex >= 0);
      internalBoundValues[internalIndex] = v;
    }

    WhereProcessor whereProcessor =
        new WhereProcessor(table, valueCodec()) {
          @Override
          protected TypedValue handleValue(String name, ColumnType type, Value<?> value) {
            return convertValue(value, name, type, values);
          }
        };

    OptionalLong optLimit = OptionalLong.empty();
    if (limit != null) {
      TypedValue v = convertValue(limit, "[limit]", Type.Bigint, values);
      int internalIndex = limit.internalIndex();
      if (internalIndex >= 0) {
        internalBoundValues[internalIndex] = v;
      }
      if (!v.isUnset()) {
        Long lvalue = (Long) v.javaValue();
        if (lvalue == null) {
          throw new IllegalArgumentException("Cannot pass null as bound value for the LIMIT");
        }
        optLimit = OptionalLong.of(lvalue);
      }
    }
    return new Bound(
        this,
        values,
        Arrays.asList(internalBoundValues),
        whereProcessor.process(whereClause),
        optLimit);
  }

  @Override
  public Query<Bound> withPreparedId(MD5Digest preparedId) {
    return new BuiltSelect(
        table(),
        valueCodec(),
        preparedId,
        executor(),
        externalQueryString,
        bindMarkers(),
        internalQueryString,
        selectedColumns,
        internalWhereValues,
        internalBindMarkers,
        whereClause,
        limit);
  }

  @Override
  public final String toString() {
    return externalQueryString;
  }

  public static class Bound extends AbstractBound<BuiltSelect> implements BoundSelect {
    private final @Nullable RowsImpacted selectedRows;
    private final OptionalLong limit;

    private Bound(
        BuiltSelect builtQuery,
        List<TypedValue> boundedValues,
        List<TypedValue> values,
        @Nullable RowsImpacted selectedRows,
        OptionalLong limit) {
      super(builtQuery, boundedValues, values);
      this.selectedRows = selectedRows;
      this.limit = limit;
    }

    @Override
    public Table table() {
      return source().query().table();
    }

    @Override
    public Set<Column> selectedColumns() {
      return source().query().selectedColumns();
    }

    @Override
    public Optional<RowsImpacted> selectedRows() {
      return Optional.ofNullable(selectedRows);
    }

    private String addColumnsToQueryString(
        Set<Column> toAdd, String queryString, boolean isStarSelect) {
      StringBuilder sb = new StringBuilder();
      if (isStarSelect) {
        int idx = queryString.indexOf('*');
        Preconditions.checkState(idx > 0, "Should have found '*' in %s", queryString);
        sb.append(queryString, 0, idx);
        sb.append(toAdd.stream().map(Column::cqlName).collect(Collectors.joining(", ")));
        // +1 to skip the '*'
        sb.append(queryString, idx + 1, queryString.length());
      } else {
        int idx = queryString.indexOf(" FROM ");
        Preconditions.checkState(idx > 0, "Should have found 'FROM' in %s", queryString);
        sb.append(queryString, 0, idx);
        for (Column c : toAdd) {
          sb.append(", ").append(c.cqlName());
        }
        sb.append(queryString, idx, queryString.length());
      }
      return sb.toString();
    }

    @Override
    public BoundSelect withAddedSelectedColumns(Set<Column> columns) {
      Set<Column> toAdd =
          columns.stream().filter(c -> !selectedColumns().contains(c)).collect(Collectors.toSet());
      if (toAdd.isEmpty()) {
        return this;
      }

      Set<Column> newSelectedColumns = new HashSet<>(selectedColumns());
      newSelectedColumns.addAll(toAdd);

      BuiltSelect oldBuilt = source().query();
      BuiltSelect newBuilt =
          new BuiltSelect(
              table(),
              oldBuilt.valueCodec(),
              null,
              oldBuilt.executor(),
              addColumnsToQueryString(toAdd, oldBuilt.externalQueryString, isStarSelect()),
              oldBuilt.bindMarkers(),
              addColumnsToQueryString(toAdd, oldBuilt.internalQueryString, isStarSelect()),
              newSelectedColumns,
              oldBuilt.internalWhereValues,
              oldBuilt.internalBindMarkers,
              oldBuilt.whereClause,
              limit.isPresent() ? Value.of(limit.getAsLong()) : null);
      return new Bound(newBuilt, source().values(), values(), selectedRows, limit);
    }

    @Override
    public OptionalLong limit() {
      return limit;
    }
  }
}
