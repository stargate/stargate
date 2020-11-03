package io.stargate.core.activator;

public class TestService {
  private final DependentService1 dependentService1;
  private final DependentService2 dependentService2;

  public TestService(DependentService1 dependentService1, DependentService2 dependentService2) {

    this.dependentService1 = dependentService1;
    this.dependentService2 = dependentService2;
  }

  public DependentService1 getDependentService1() {
    return dependentService1;
  }

  public DependentService2 getDependentService2() {
    return dependentService2;
  }
}
