package io.stargate.bridge.service.docsapi;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class PagingStateTracker {
  private final ArrayList<PagingStateSupplier> states;

  public PagingStateTracker(List<ByteBuffer> initialStates) {
    states = new ArrayList<>(initialStates.size());
    initialStates.stream().map(PagingStateSupplier::fixed).forEach(states::add);
  }

  public Accumulator combine(Accumulator prev, Accumulator next) {
    return prev.combine(this, next);
  }

  public void track(DocProperty row) {
    states.set(row.queryIndex(), row);
  }

  public List<PagingStateSupplier> slice() {
    return new ArrayList<>(states);
  }
}
