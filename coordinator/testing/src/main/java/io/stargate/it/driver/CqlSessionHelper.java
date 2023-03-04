package io.stargate.it.driver;

import com.datastax.oss.driver.api.core.CqlSession;

/**
 * Utility methods to work with sessions in Stargate tests. To use this object, declare it as a
 * parameter of a test method and {@link CqlSessionExtension} will inject it.
 */
public interface CqlSessionHelper {

  /**
   * Waits until all Stargate nodes are present in the driver's metadata. You only need this if you
   * create sessions manually in tests. If you use the session created by the extension, it has
   * already been done internally.
   *
   * <p>The driver discovers nodes through gossip events and {@code system.peers}. CI environments
   * can be slow, and it sometimes take a little while before the information replicates. This
   * method ensures that we don't start a test too early if it depends on all nodes being visible
   * from the driver.
   */
  void waitForStargateNodes(CqlSession session);
}
