package io.stargate.app;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.annotations.QuarkusMain;

/**
 * Serves only so app can be started from the IDE directly.
 */
@QuarkusMain
public class App {

  public static void main(String[] args) {
    Quarkus.run(args);
  }
}
