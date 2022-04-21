package ai.datasqrl.config.util;

import ai.datasqrl.config.error.ErrorCollector;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

public class ConfigurationUtil {

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

  public static <C> ErrorCollector javaxValidate(C configuration) {
    ErrorCollector errors = ErrorCollector.root();
    javaxValidate(configuration, errors);
    return errors;
  }

}
