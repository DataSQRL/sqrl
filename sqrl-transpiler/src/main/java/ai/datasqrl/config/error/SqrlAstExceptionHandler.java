package ai.datasqrl.config.error;

import ai.datasqrl.config.error.ErrorLocation.File;
import ai.datasqrl.config.error.ErrorMessage.Implementation;
import ai.datasqrl.config.error.ErrorMessage.Severity;
import ai.datasqrl.parse.SqrlAstException;
import java.util.Optional;

public class SqrlAstExceptionHandler implements ErrorHandler<SqrlAstException> {

  @Override
  public ErrorMessage handle(SqrlAstException e, ErrorEmitter emitter) {
    return new Implementation(Optional.of(e.getErrorCode()), e.getMessage(),
        emitter.getBaseLocation()
            .atFile(new File(e.getPos().getLineNum(), e.getPos().getColumnNum())),
        Severity.FATAL);
  }
}
