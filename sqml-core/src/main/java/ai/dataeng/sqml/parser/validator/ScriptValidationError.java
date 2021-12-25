package ai.dataeng.sqml.parser.validator;

import ai.dataeng.sqml.type.basic.ProcessMessage;
import lombok.Value;

@Value
public class ScriptValidationError implements ProcessMessage {
  String message;
  Severity severity;
}
