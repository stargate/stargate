package io.stargate.db.tracing;

import io.stargate.db.datastore.Row;
import java.util.List;

public class TracingData {
  private final List<Row> events;
  private final Row session;

  public TracingData(List<Row> events, Row session) {
    this.events = events;
    this.session = session;
  }

  public List<Row> getEvents() {
    return events;
  }

  public Row getSessionRow() {
    return session;
  }
}
