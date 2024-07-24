package io.stargate.it.driver;

import java.lang.annotation.*;

@Target({ElementType.TYPE, ElementType.METHOD})
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface WithProtocolVersion {
  String value() default "";
}
