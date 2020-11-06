package io.stargate.db;

import io.stargate.config.store.api.ConfigStore;
import io.stargate.core.activator.BaseActivator;
import io.stargate.core.metrics.api.Metrics;
import io.stargate.db.cdc.CDCProducer;
import io.stargate.db.cdc.CDCService;
import io.stargate.db.cdc.CDCServiceImpl;
import java.util.Arrays;
import java.util.List;
import javax.annotation.Nullable;

public class DbActivator extends BaseActivator {
  private ServicePointer<Metrics> metrics;
  private ServicePointer<CDCProducer> cdcProducer;
  private ServicePointer<ConfigStore> configStore;
  private CDCService cdcService;

  public DbActivator() {
    super("CDC service");
  }

  @Nullable
  @Override
  protected ServiceAndProperties createService() {
    cdcService = new CDCServiceImpl(cdcProducer.get(), metrics.get(), configStore.get());
    return null;
  }

  @Override
  protected void stopService() {
    try {
      cdcService.close();
    } catch (Exception e) {
      throw new CDCCloseException("Problem when closing CDC service", e);
    }
  }

  @Override
  protected List<ServicePointer<?>> dependencies() {
    return Arrays.asList(cdcProducer, metrics, configStore);
  }

  public static class CDCCloseException extends RuntimeException {
    public CDCCloseException(String message, Exception e) {
      super(message, e);
    }
  }
}
