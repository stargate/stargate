package io.stargate.db.query;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Specify which rows are updated by a given DML query.
 *
 * <p>There is effectively two implementation of this class, corresponding to the 2 ways rows can be
 * updated in CQL: either a set of specific rows (implemented by {@link Keys}), or a range of rows
 * (implemented by {@link Ranges}). Note that in practice, ranges are allowed by DELETE in CQL so
 * far, and INSERT can only insert a single row at a time.
 */
public abstract class RowsImpacted {
  private @Nullable Set<PartitionKey> partitionKeys; // Lazily Computed

  private RowsImpacted() {}

  public abstract boolean isRanges();

  public boolean isKeys() {
    return !isRanges();
  }

  public Keys asKeys() {
    Preconditions.checkState(isKeys());
    return (Keys) this;
  }

  public Ranges asRanges() {
    Preconditions.checkState(isRanges());
    return (Ranges) this;
  }

  public Set<PartitionKey> partitionKeys() {
    if (partitionKeys == null) {
      partitionKeys = computePartitionKeys();
    }
    return partitionKeys;
  }

  protected abstract Set<PartitionKey> computePartitionKeys();

  public static final class Keys extends RowsImpacted {
    private final List<PrimaryKey> primaryKeys;

    public Keys(List<PrimaryKey> primaryKeys) {
      this.primaryKeys = primaryKeys;
    }

    public List<PrimaryKey> primaryKeys() {
      return primaryKeys;
    }

    @Override
    public boolean isRanges() {
      return false;
    }

    @Override
    protected Set<PartitionKey> computePartitionKeys() {
      // Minor optimization, but that's a common case in practice.
      if (primaryKeys.size() == 1) {
        return Collections.singleton(primaryKeys.get(0).partitionKey());
      }

      Set<PartitionKey> partitionKeys = new HashSet<>();
      primaryKeys.forEach(pk -> partitionKeys.add(pk.partitionKey()));
      return partitionKeys;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Keys)) {
        return false;
      }
      Keys keys = (Keys) o;
      return primaryKeys.equals(keys.primaryKeys);
    }

    @Override
    public int hashCode() {
      return Objects.hash(primaryKeys);
    }

    @Override
    public String toString() {
      return primaryKeys.toString();
    }
  }

  public static final class Ranges extends RowsImpacted {
    private final List<RowsRange> rowsRanges;

    public Ranges(List<RowsRange> rowsRanges) {
      this.rowsRanges = rowsRanges;
    }

    public List<RowsRange> ranges() {
      return rowsRanges;
    }

    @Override
    public boolean isRanges() {
      return true;
    }

    @Override
    protected Set<PartitionKey> computePartitionKeys() {
      Set<PartitionKey> partitionKeys = new HashSet<>();
      rowsRanges.forEach(pk -> partitionKeys.add(pk.partitionKey()));
      return partitionKeys;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof Ranges)) {
        return false;
      }
      Ranges ranges = (Ranges) o;
      return rowsRanges.equals(ranges.rowsRanges);
    }

    @Override
    public int hashCode() {
      return Objects.hash(rowsRanges);
    }
  }
}
