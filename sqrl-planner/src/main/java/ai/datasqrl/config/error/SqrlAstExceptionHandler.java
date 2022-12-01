package ai.datasqrl.config.error;

import ai.datasqrl.config.error.ErrorLocation.FileLocation;
import ai.datasqrl.config.error.ErrorMessage.Implementation;
import ai.datasqrl.config.error.ErrorMessage.Severity;
import ai.datasqrl.parse.SqrlAstException;
import java.util.Optional;

public class SqrlAstExceptionHandler implements ErrorHandler<SqrlAstException> {

  @Override
  public ErrorMessage handle(SqrlAstException e, ErrorEmitter emitter) {
    return new Implementation(Optional.ofNullable(e.getErrorCode()), e.getMessage(),
        emitter.getBaseLocation()
            .atFile(new FileLocation(e.getPos().getLineNum(), e.getPos().getColumnNum())),
        Severity.FATAL,
        emitter.getSourceMap());
  }

  @Override
  public Class getHandleClass() {
    return SqrlAstException.class;
  }
}
