/**
 * Copyright DataStax, Inc.
 *
 * <p>Please see the included license file for details.
 */
package com.datastax.bdp.search.solr.core;

public class CassandraCoreRequestOptions {
  private volatile boolean distributed;
  private volatile boolean slave;
  private volatile boolean recovery;
  private volatile boolean deleteAll;
  private volatile boolean reindex;
  private volatile boolean deleteDataDir;
  private volatile boolean deleteResources;

  public boolean isDistributed() {
    return distributed;
  }

  public boolean isRecovery() {
    return recovery;
  }

  public boolean isDeleteAll() {
    return deleteAll;
  }

  public boolean isReindex() {
    return reindex;
  }

  public void setDistributed(boolean distributed) {
    this.distributed = distributed;
  }

  public static class Builder {
    private final CassandraCoreRequestOptions result;

    public Builder() {
      this.result = new CassandraCoreRequestOptions();
    }

    public Builder setDistributed(boolean distributed) {
      result.distributed = distributed;
      return this;
    }

    public Builder setSlave(boolean slave) {
      result.slave = slave;
      return this;
    }

    public Builder setRecovery(boolean recovery) {
      result.recovery = recovery;
      return this;
    }

    public Builder setDeleteAll(boolean deleteAll) {
      result.deleteAll = deleteAll;
      return this;
    }

    public Builder setReindex(boolean reindex) {
      result.reindex = reindex;
      return this;
    }

    public CassandraCoreRequestOptions build() {
      return result;
    }
  }
}
