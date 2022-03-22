package ai.dataeng.sqml.config.util;

import ai.dataeng.sqml.config.ConfigurationError;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import java.util.Set;
import javax.validation.ConstraintViolation;
import javax.validation.Validation;
import javax.validation.Validator;
import javax.validation.ValidatorFactory;

public class ConfigurationUtil {

    public static<C> boolean javaxValidate(C configuration, ProcessMessage.ProcessBundle<ConfigurationError> errors) {
        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        Validator validator = factory.getValidator();
        Set<ConstraintViolation<C>> violations = validator.validate(configuration);
        boolean isvalid = true;
        for (ConstraintViolation<C> violation : violations) {
            isvalid = false;
            errors.add(ConfigurationError.fatal(ConfigurationError.LocationType.GLOBAL,
                    violation.getPropertyPath().toString(),violation.getMessage() + ", but found: %s",violation.getInvalidValue()));
        }
        return isvalid;
    }

    public static<C> ProcessMessage.ProcessBundle<ConfigurationError> javaxValidate(C configuration) {
        ProcessMessage.ProcessBundle<ConfigurationError> errors = new ProcessMessage.ProcessBundle<>();
        javaxValidate(configuration,errors);
        return errors;
    }

}
