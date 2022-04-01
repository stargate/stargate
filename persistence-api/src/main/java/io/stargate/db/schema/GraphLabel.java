package io.stargate.db.schema;

import java.io.Serializable;

/**
 * The name of a graph "vertex label" or "edge label".
 *
 * <p>When declaring a label, providing a name is optional for the user. If a name is not provided,
 * the name of the table on which the label is defined is reused as label name by default.
 */
public abstract class GraphLabel implements Serializable {
  public abstract String name();
}
