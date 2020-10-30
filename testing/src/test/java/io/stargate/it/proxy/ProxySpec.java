package io.stargate.it.proxy;

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.TYPE})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface ProxySpec {
  String localAddress() default "127.0.1.11";

  int localPort() default 9043;

  String remoteAddress() default "127.0.0.11";

  int remotePort() default 9043;
}
