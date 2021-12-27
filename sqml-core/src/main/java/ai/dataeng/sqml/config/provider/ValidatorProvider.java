package ai.dataeng.sqml.config.provider;

import ai.dataeng.sqml.parser.validator.Validator;

public interface ValidatorProvider {
  Validator getValidator();
}
