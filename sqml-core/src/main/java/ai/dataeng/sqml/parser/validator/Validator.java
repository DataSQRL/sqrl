package ai.dataeng.sqml.parser.validator;

import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.config.error.ErrorMessage;
import ai.dataeng.sqml.config.error.ErrorCollector;

public interface Validator {

  ErrorCollector validate(ScriptNode scriptNode);
}
