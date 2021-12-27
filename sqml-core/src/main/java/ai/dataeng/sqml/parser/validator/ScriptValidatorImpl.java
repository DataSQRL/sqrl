package ai.dataeng.sqml.parser.validator;

import ai.dataeng.sqml.tree.ScriptNode;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import ai.dataeng.sqml.type.basic.ProcessMessage.ProcessBundle;

public class ScriptValidatorImpl implements Validator {

  @Override
  public ProcessBundle<ProcessMessage> validate(ScriptNode scriptNode) {
    return new ProcessBundle<>();
  }
}
