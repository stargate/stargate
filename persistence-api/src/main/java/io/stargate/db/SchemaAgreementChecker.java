package io.stargate.db;

import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.concurrent.TimeUnit;

/** A persistence object that checks whether it's in schema agreement. */
public interface SchemaAgreementChecker {
  int SCHEMA_AGREEMENT_WAIT_RETRIES =
      Integer.getInteger("stargate.persistence.schema.agreement.wait.retries", 900);

  /**
   * Checks whether this coordinator in schema agreement with the other nodes in the cluster.
   *
   * @return true if schema is agreement; otherwise false.
   */
  boolean isInSchemaAgreement();

  /** Wait for schema to agree across the cluster */
  default void waitForSchemaAgreement() {
    for (int count = 0; count < SCHEMA_AGREEMENT_WAIT_RETRIES; count++) {
      if (isInSchemaAgreement()) {
        return;
      }
      Uninterruptibles.sleepUninterruptibly(200, TimeUnit.MILLISECONDS);
    }
    throw new IllegalStateException(
        "Failed to reach schema agreement after "
            + (200 * SCHEMA_AGREEMENT_WAIT_RETRIES)
            + " milliseconds.");
  }
}
