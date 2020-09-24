/*
 * Copyright The Stargate Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.stargate.it;

import io.stargate.starter.Starter;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.agent.builder.AgentBuilder;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.objenesis.Objenesis;
import org.objenesis.ObjenesisStd;
import org.osgi.framework.BundleException;
import org.osgi.framework.InvalidSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class does some magic to allow access to OSGi loaded services for testing. The common
 * assumption is the classes loaded for testing in the local classpath are 1:1 with the classes
 * loaded in the services.
 *
 * <p>Under that assumption when a service is loaded it is "proxied"
 *
 * <p>ClassLoader 1 | Class A < -- Proxy -- > ClassLoader 2 | Class A
 *
 * <p>The unit test calls getOsgiService() which creates a proxy of the instance. Then when a call
 * is made to the proxy it intercepts the call and converts the passed arguments into a proxy for
 * OSGI's ClassPath version of the arguments and executes the call under the actual instance. The
 * result is then proxied back to the unit tests classpath version of the result class.
 *
 * <p>Special care is taken to handle collections, futures and enumerations.
 */
public class BaseOsgiIntegrationTest extends BaseStorageIntegrationTest {
  private static final Logger logger = LoggerFactory.getLogger(BaseOsgiIntegrationTest.class);

  static {
    unFinal();
  }

  static Starter stargateStarter;
  public static String stargateHost = "127.0.0.11";
  public boolean enableAuth;

  /** Remove final from all internal classes to allow for proxying */
  public static void unFinal() {
    ByteBuddyAgent.install();
    new AgentBuilder.Default()
        .type(
            ElementMatchers.isFinal()
                .and(ElementMatchers.not(ElementMatchers.isEnum()))
                .and(ElementMatchers.nameContainsIgnoreCase("io.stargate.")))
        .transform(
            (typeBuilder, td, cl, jm) ->
                typeBuilder.modifiers(TypeManifestation.PLAIN).modifiers(Visibility.PUBLIC))
        .installOnByteBuddyAgent();
  }

  /**
   * The interceptor class used to proxy between two instances of the same class loaded under
   * different classloaders.
   */
  public class ProxyInterceptor {

    final Object obj;
    final ClassLoader sourceLoader;
    final ClassLoader destLoader;

    public ProxyInterceptor(Object obj, ClassLoader sourceLoader, ClassLoader destLoader) {
      this.obj = obj;
      this.sourceLoader = sourceLoader;
      this.destLoader = destLoader;
    }

    @RuntimeType
    public Object intercept(@AllArguments Object[] args, @Origin Method method) throws Throwable {

      try {
        final Class<?>[] mappedArgTypes = new Class<?>[args == null ? 0 : args.length];
        final Object[] mappedArgs = new Object[mappedArgTypes.length];
        final Class<?>[] sourceTypes = method.getParameterTypes();

        for (int i = 0; args != null && i < mappedArgTypes.length; i++) {
          if (sourceTypes[i].getClassLoader() == null) {
            mappedArgTypes[i] = sourceTypes[i];
            mappedArgs[i] = proxyValue(args[i], destLoader, sourceLoader);
          } else {
            mappedArgTypes[i] = sourceLoader.loadClass(sourceTypes[i].getName());
            mappedArgs[i] = proxy(args[i], destLoader, sourceLoader, mappedArgTypes[i]);
          }
        }

        final Method realMethod = obj.getClass().getMethod(method.getName(), mappedArgTypes);
        final Object result = realMethod.invoke(obj, mappedArgs);
        if (method.getReturnType().getClassLoader() == null
            || result == null
            || !hasClass(destLoader, method.getReturnType())) {
          return proxyValue(result, sourceLoader, destLoader);
        }

        return proxy(result, sourceLoader, destLoader, method.getReturnType());
      } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
        // Throw the root exception
        Throwable t = e;
        while (t.getCause() != null) {
          t = t.getCause();
        }

        throw t;
      }
    }
  }

  /** Tests if a class is available in a given classloader */
  boolean hasClass(ClassLoader classLoader, Class clazz) {
    try {
      classLoader.loadClass(clazz.getName());
      return true;
    } catch (ClassNotFoundException e) {
      return false;
    }
  }

  /**
   * Converts a result of a method loaded under a source classloader and converts it to a
   * destination loader
   */
  Object proxyValue(Object obj, ClassLoader sourceLoader, ClassLoader destLoader)
      throws ClassNotFoundException {
    if (obj == null) return obj;

    if (obj instanceof CompletableFuture)
      return ((CompletableFuture) obj)
          .thenApply(p -> proxy(p, sourceLoader, destLoader, p.getClass()));

    if (obj.getClass().isEnum())
      return Enum.valueOf(
          (Class<Enum>) destLoader.loadClass(obj.getClass().getName()), obj.toString());

    if (obj instanceof Collection) {
      Collection<Object> proxyCollection;
      if (obj instanceof List) proxyCollection = new ArrayList<>();
      else proxyCollection = new HashSet<>();

      for (Object o : (Collection) obj) {
        if (o.getClass().getClassLoader() == null || !hasClass(destLoader, o.getClass())) {
          proxyCollection.add(o);
        } else {
          proxyCollection.add(
              proxy(o, sourceLoader, destLoader, destLoader.loadClass(o.getClass().getName())));
        }
      }

      return proxyCollection;
    }

    if (obj instanceof Iterator) {
      Iterator it = (Iterator) obj;
      return new Iterator() {
        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public Object next() {
          Object o = it.next();
          try {
            return proxy(o, sourceLoader, destLoader, destLoader.loadClass(o.getClass().getName()));
          } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }

    return obj;
  }

  /**
   * Creates a proxy for an object obj loaded in sourceLoader hierarchy that is visible in
   * destLoader as destClass. Assumes all methods of destClass are implemented in obj exactly with
   * same signature
   */
  Object proxy(
      final Object obj,
      final ClassLoader sourceLoader,
      final ClassLoader destLoader,
      final Class<?> destClass) {

    if (obj == null) return obj;

    if (destClass.isEnum()) return Enum.valueOf((Class) destClass, ((Enum) obj).name());

    Class<?> dynamicType =
        new ByteBuddy()
            .subclass(destClass)
            .method(ElementMatchers.any())
            .intercept(MethodDelegation.to(new ProxyInterceptor(obj, sourceLoader, destLoader)))
            .make()
            .load(destLoader)
            .getLoaded();

    // Need to use this to handle making a new instance without default constructor
    Objenesis objenesis = new ObjenesisStd();
    return objenesis.newInstance(dynamicType);
  }

  /** Returns a proxy facade for a OSGi service loaded in stargate */
  public <T> T getOsgiService(String name, Class<T> clazz) throws InvalidSyntaxException {
    Object p =
        stargateStarter.getService(name).orElseThrow(() -> new AssertionError("Missing " + name));

    return (T) proxy(p, p.getClass().getClassLoader(), clazz.getClassLoader(), clazz);
  }

  @BeforeEach
  public void startOsgi() throws BundleException {
    if (stargateStarter != null) {
      return;
    }

    // Start stargate and get the persistance object
    stargateStarter =
        new Starter(
            CLUSTER_NAME,
            clusterVersion(),
            stargateHost,
            seedAddress(),
            storagePort(),
            datacenter(),
            rack(),
            isDse(),
            !isDse(),
            9043);

    System.setProperty("stargate.auth_api_enable_username_token", "true");
    stargateStarter.withAuthEnabled(enableAuth).start();
  }
}
