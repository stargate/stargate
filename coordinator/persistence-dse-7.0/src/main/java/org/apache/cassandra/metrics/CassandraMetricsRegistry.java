package org.apache.cassandra.metrics;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import java.lang.reflect.Method;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

/**
 * Patches the DSE metric implementation to allow injecting our own registry.
 *
 * <p>This is messy because of how the original class was designed (no interface contract, relies on
 * static state...): {@link #actualRegistry} must be set manually before hitting any Cassandra code
 * that might attempt to register a metric.
 *
 * <p>This class must mimic the exact public API of the original one.
 */
public class CassandraMetricsRegistry extends MetricRegistry {

  public static volatile MetricRegistry actualRegistry = new MetricRegistry();
  public static final CassandraMetricsRegistry Metrics = new CassandraMetricsRegistry();

  private CassandraMetricsRegistry() {
    super();
  }

  public Counter counter(MetricName name, boolean isComposite) {
    return register(name, Counter.make(isComposite));
  }

  public Counter counter(MetricName name) {
    return counter(name, false);
  }

  public Counter counter(MetricName name, MetricName alias, boolean isComposite) {
    return counter(name, isComposite);
  }

  public Meter meter(MetricName name) {
    return register(name, Meter.makeSingle());
  }

  public Meter meter(MetricName name, boolean isComposite) {
    return register(name, isComposite ? Meter.makeComposite() : Meter.makeSingle());
  }

  public Meter meter(MetricName name, MetricName alias) {
    return meter(name);
  }

  public Histogram histogram(MetricName name, boolean considerZeroes) {
    return histogram(name, considerZeroes, false);
  }

  public Histogram histogram(MetricName name, boolean considerZeroes, boolean isComposite) {
    return register(name, Histogram.make(considerZeroes, isComposite));
  }

  public Histogram histogram(
      MetricName name, MetricName alias, boolean considerZeroes, boolean isComposite) {
    return histogram(name, considerZeroes, isComposite);
  }

  public Timer timer(MetricName name, boolean isComposite) {
    return register(name, new Timer(isComposite));
  }

  public Timer timer(MetricName name) {
    return timer(name, false);
  }

  public Timer timer(MetricName name, MetricName alias, boolean isComposite) {
    return timer(name, isComposite);
  }

  public <T extends Metric> T register(MetricName name, T metric) {
    try {
      return actualRegistry.register(name.getMetricName(), metric);
    } catch (IllegalArgumentException e) {
      @SuppressWarnings("unchecked")
      T existing = (T) actualRegistry.getMetrics().get(name.getMetricName());
      return existing;
    }
  }

  public <T extends Metric> T register(MetricName name, MetricName aliasName, T metric) {
    return register(name, metric);
  }

  public boolean remove(MetricName name) {
    return actualRegistry.remove(name.getMetricName());
  }

  public boolean remove(MetricName name, MetricName alias) {
    return remove(name);
  }

  public void registerMBean(Metric metric, ObjectName name) {
    // Nothing to do, we don't register MBeans
  }

  public void registerAlias(MetricName existingName, MetricName aliasName) {
    // Nothing to do, we don't require aliases for legacy names
  }

  public interface MetricMBean {
    ObjectName objectName();
  }

  public interface JmxGaugeMBean extends MetricMBean {
    Object getValue();
  }

  public interface JmxHistogramMBean extends MetricMBean {
    long getCount();

    long getMin();

    long getMax();

    double getMean();

    double getStdDev();

    double get50thPercentile();

    double get75thPercentile();

    double get95thPercentile();

    double get98thPercentile();

    double get99thPercentile();

    double get999thPercentile();

    long[] values();

    long[] getRecentValues();
  }

  public interface JmxCounterMBean extends MetricMBean {
    long getCount();
  }

  public interface JmxMeterMBean extends MetricMBean {
    long getCount();

    double getMeanRate();

    double getOneMinuteRate();

    double getFiveMinuteRate();

    double getFifteenMinuteRate();

    String getRateUnit();
  }

  public interface JmxTimerMBean extends JmxMeterMBean {
    double getMin();

    double getMax();

    double getMean();

    double getStdDev();

    double get50thPercentile();

    double get75thPercentile();

    double get95thPercentile();

    double get98thPercentile();

    double get99thPercentile();

    double get999thPercentile();

    long[] values();

    long[] getRecentValues();

    String getDurationUnit();
  }

  /** A value class encapsulating a metric's owning class and name. */
  public static final class MetricName implements Comparable<MetricName> {
    private final String group;
    private final String type;
    private final String name;
    private final String scope;
    private final String mBeanName;

    /**
     * Creates a new {@link MetricName} without a scope.
     *
     * @param klass the {@link Class} to which the {@link Metric} belongs
     * @param name the name of the {@link Metric}
     */
    public MetricName(Class<?> klass, String name) {
      this(klass, name, null);
    }

    /**
     * Creates a new {@link MetricName} without a scope.
     *
     * @param group the group to which the {@link Metric} belongs
     * @param type the type to which the {@link Metric} belongs
     * @param name the name of the {@link Metric}
     */
    public MetricName(String group, String type, String name) {
      this(group, type, name, null);
    }

    /**
     * Creates a new {@link MetricName} without a scope.
     *
     * @param klass the {@link Class} to which the {@link Metric} belongs
     * @param name the name of the {@link Metric}
     * @param scope the scope of the {@link Metric}
     */
    public MetricName(Class<?> klass, String name, String scope) {
      this(
          klass.getPackage() == null ? "" : klass.getPackage().getName(),
          withoutFinalDollar(klass.getSimpleName()),
          name,
          scope);
    }

    /**
     * Creates a new {@link MetricName} without a scope.
     *
     * @param group the group to which the {@link Metric} belongs
     * @param type the type to which the {@link Metric} belongs
     * @param name the name of the {@link Metric}
     * @param scope the scope of the {@link Metric}
     */
    public MetricName(String group, String type, String name, String scope) {
      this(group, type, name, scope, createMBeanName(group, type, name, scope));
    }

    /**
     * Creates a new {@link MetricName} without a scope.
     *
     * @param group the group to which the {@link Metric} belongs
     * @param type the type to which the {@link Metric} belongs
     * @param name the name of the {@link Metric}
     * @param scope the scope of the {@link Metric}
     * @param mBeanName the 'ObjectName', represented as a string, to use when registering the
     *     MBean.
     */
    public MetricName(String group, String type, String name, String scope, String mBeanName) {
      if (group == null || type == null) {
        throw new IllegalArgumentException("Both group and type need to be specified");
      }
      if (name == null) {
        throw new IllegalArgumentException("Name needs to be specified");
      }
      this.group = group;
      this.type = type;
      this.name = name;
      this.scope = scope;
      this.mBeanName = mBeanName;
    }

    /**
     * Returns the group to which the {@link Metric} belongs. For class-based metrics, this will be
     * the package name of the {@link Class} to which the {@link Metric} belongs.
     *
     * @return the group to which the {@link Metric} belongs
     */
    public String getGroup() {
      return group;
    }

    /**
     * Returns the type to which the {@link Metric} belongs. For class-based metrics, this will be
     * the simple class name of the {@link Class} to which the {@link Metric} belongs.
     *
     * @return the type to which the {@link Metric} belongs
     */
    public String getType() {
      return type;
    }

    /**
     * Returns the name of the {@link Metric}.
     *
     * @return the name of the {@link Metric}
     */
    public String getName() {
      return name;
    }

    public String getMetricName() {
      return MetricRegistry.name(group, type, name, scope);
    }

    /**
     * Returns the scope of the {@link Metric}.
     *
     * @return the scope of the {@link Metric}
     */
    public String getScope() {
      return scope;
    }

    /**
     * Returns {@code true} if the {@link Metric} has a scope, {@code false} otherwise.
     *
     * @return {@code true} if the {@link Metric} has a scope
     */
    public boolean hasScope() {
      return scope != null;
    }

    /**
     * Returns the MBean name for the {@link Metric} identified by this metric name.
     *
     * @return the MBean name
     */
    public ObjectName getMBeanName() {

      String mname = mBeanName;

      if (mname == null) mname = getMetricName();

      try {

        return new ObjectName(mname);
      } catch (MalformedObjectNameException e) {
        try {
          return new ObjectName(ObjectName.quote(mname));
        } catch (MalformedObjectNameException e1) {
          throw new RuntimeException(e1);
        }
      }
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final MetricName that = (MetricName) o;
      return mBeanName.equals(that.mBeanName);
    }

    @Override
    public int hashCode() {
      return mBeanName.hashCode();
    }

    @Override
    public String toString() {
      return mBeanName;
    }

    @Override
    public int compareTo(MetricName o) {
      return mBeanName.compareTo(o.mBeanName);
    }

    private static String createMBeanName(String group, String type, String name, String scope) {
      final StringBuilder nameBuilder = new StringBuilder();
      nameBuilder.append(ObjectName.quote(group));
      nameBuilder.append(":type=");
      nameBuilder.append(ObjectName.quote(type));
      if (scope != null) {
        nameBuilder.append(",scope=");
        nameBuilder.append(ObjectName.quote(scope));
      }
      if (name.length() > 0) {
        nameBuilder.append(",name=");
        nameBuilder.append(ObjectName.quote(name));
      }
      return nameBuilder.toString();
    }

    /**
     * If the group is empty, use the package name of the given class. Otherwise use group
     *
     * @param group The group to use by default
     * @param klass The class being tracked
     * @return a group for the metric
     */
    public static String chooseGroup(String group, Class<?> klass) {
      if (group == null || group.isEmpty()) {
        group = klass.getPackage() == null ? "" : klass.getPackage().getName();
      }
      return group;
    }

    /**
     * If the type is empty, use the simple name of the given class. Otherwise use type
     *
     * @param type The type to use by default
     * @param klass The class being tracked
     * @return a type for the metric
     */
    public static String chooseType(String type, Class<?> klass) {
      if (type == null || type.isEmpty()) {
        type = withoutFinalDollar(klass.getSimpleName());
      }
      return type;
    }

    /**
     * If name is empty, use the name of the given method. Otherwise use name
     *
     * @param name The name to use by default
     * @param method The method being tracked
     * @return a name for the metric
     */
    public static String chooseName(String name, Method method) {
      if (name == null || name.isEmpty()) {
        name = method.getName();
      }
      return name;
    }
  }

  /**
   * Strips a single final '$' from input
   *
   * @param s String to strip
   * @return a string with one less '$' at end
   */
  private static String withoutFinalDollar(String s) {
    int l = s.length();
    return (l != 0 && '$' == s.charAt(l - 1)) ? s.substring(0, l - 1) : s;
  }
}
