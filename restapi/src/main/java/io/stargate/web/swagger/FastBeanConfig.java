package io.stargate.web.swagger;

import com.google.common.reflect.TypeToken;
import io.stargate.web.RestApiActivator;
import io.swagger.annotations.Api;
import io.swagger.annotations.SwaggerDefinition;
import io.swagger.jaxrs.config.BeanConfig;
import java.net.URL;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.osgi.framework.Bundle;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.wiring.BundleWiring;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

public class FastBeanConfig extends BeanConfig {

  @Override
  public Set<Class<?>> classes() {
    ConfigurationBuilder config = new ConfigurationBuilder();

    Set<Class<?>> output = new HashSet<>();
    if (getResourcePackage() != null && !"".equals(getResourcePackage())) {
      String[] parts = getResourcePackage().split(",");
      for (String pkg : parts) {
        if (!"".equals(pkg)) {
          Bundle bundle = FrameworkUtil.getBundle(RestApiActivator.class);

          BundleWiring bundleWiring = bundle.adapt(BundleWiring.class);
          List<URL> resources =
              bundleWiring.findEntries(
                  resourceName(pkg), "*.class", BundleWiring.FINDENTRIES_RECURSE);

          for (URL resource : resources) {
            // TODO: [doug] 2020-09-29, Tue, 17:17 is this tccl needed or can we use the default
            ClassLoader tccl = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(this.getClass().getClassLoader());

            try {
              String className = pkgName(resource.getPath());
              className = className.substring(0, className.indexOf(".class"));
              Class<?> cls = Class.forName(className);

              if (cls.isAnnotationPresent(javax.ws.rs.Path.class)
                  || cls.isAnnotationPresent(SwaggerDefinition.class)) {
                output.add(cls);
              }

              /*
               * Find concrete types annotated with @Api, but with a supertype annotated with @Path.
               * This would handle split resources where the interface has jax-rs annotations
               * and the implementing class has Swagger annotations
               */
              if (cls.isAnnotationPresent(Api.class)) {
                for (Class<?> intfc : TypeToken.of(cls).getTypes().interfaces().rawTypes()) {
                  if (intfc.isAnnotationPresent(javax.ws.rs.Path.class)) {
                    output.add(cls);
                    break;
                  }
                }
              }
            } catch (Exception e) {
              throw new RuntimeException(
                  "Error occurred while finding classes to generate Swagger config for", e);
            } finally {
              Thread.currentThread().setContextClassLoader(tccl);
            }
          }

          config.addUrls(ClasspathHelper.forPackage(pkg));
        }
      }
    }

    return output;
  }

  private static String resourceName(String name) {
    if (name != null) {
      String resourceName = name.replace(".", "/");
      resourceName = resourceName.replace("\\", "/");
      if (resourceName.startsWith("/")) {
        resourceName = resourceName.substring(1);
      }
      return resourceName;
    }
    return null;
  }

  private static String pkgName(String name) {
    if (name != null) {
      String resourceName = name.replace("/", ".");
      if (resourceName.startsWith(".")) {
        resourceName = resourceName.substring(1);
      }
      return resourceName;
    }
    return null;
  }
}
