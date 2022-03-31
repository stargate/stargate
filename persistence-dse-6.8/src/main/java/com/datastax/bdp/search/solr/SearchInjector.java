package com.datastax.bdp.search.solr;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class SearchInjector {
  private static volatile Injector instance;

  private static volatile AbstractModule module;

  public static Injector get() {
    if (instance == null) {
      synchronized (SearchInjector.class) {
        if (instance == null) {
          instance = Guice.createInjector(getModule());
        }
      }
    }
    return instance;
  }

  private static AbstractModule getModule() {
    if (module == null) {
      module = new SearchModule();
    }
    return module;
  }

  public static synchronized void set(Injector injector) {
    instance = injector;
  }

  public static void setModule(AbstractModule _module) {
    module = _module;
  }
}
