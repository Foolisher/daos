package io.terminus.daos.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * <pre>
 *  Restfull api definition
 * </pre>
 *
 * @author wanggen on 2015-03-19.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface RequestMapping {
    String value();
    String desc() default "";
}
