package ai.datasqrl.parse;

import ai.datasqrl.config.error.ErrorEmitter;
import ai.datasqrl.config.error.ErrorHandler;
import ai.datasqrl.config.error.ErrorMessage;

public class ParsingExceptionHandler implements ErrorHandler<ParsingException> {

  @Override
  public ErrorMessage handle(ParsingException e, ErrorEmitter emitter) {
    return emitter.fatal(e.getLineNumber(), e.getColumnNumber(), e.getErrorMessage(),
        e.getCause());
  }

  @Override
  public Class getHandleClass() {
    return ParsingException.class;
  }
}
