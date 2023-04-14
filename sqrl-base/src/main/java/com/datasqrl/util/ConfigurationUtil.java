/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.util;

import com.datasqrl.error.ErrorCollector;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

public class ConfigurationUtil {

  /**
   * Validates the provided configuration object using JavaX validation and returns true if the
   * configuration object is valid (i.e. satisfies all constraints), else false.
   * <p>
   * Any errors are added to the provided error collector.
   *
   * @param configuration the configuration to validate
   * @param errors        collector for validation errors
   * @param <C>           the type of configuration
   * @return true if valid, else false
   */
  public static <C> boolean javaxValidate(C configuration, ErrorCollector errors) {
    ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
    Validator validator = factory.getValidator();
    Set<ConstraintViolation<C>> violations = validator.validate(configuration);
    boolean isvalid = true;
    for (ConstraintViolation<C> violation : violations) {
      isvalid = false;
      errors.resolve(violation.getPropertyPath().toString())
          .fatal(violation.getMessage() + ", but found: %s", violation.getInvalidValue());
    }
    return isvalid;
  }


}
