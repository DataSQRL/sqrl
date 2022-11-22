package ai.datasqrl.config.error;

import io.vertx.json.schema.ValidationException;
import org.apache.commons.lang3.StringUtils;

public class ValidationExceptionHandler implements ErrorHandler<ValidationException> {

  @Override
  public ErrorMessage handle(ValidationException exception, ErrorEmitter emitter) {
    String msg = exception.getMessage() + " [ keyword = " + exception.keyword() + "]";
    //Convert JSON location to error location. The only access we have to the JSONPointer path is through toString() and we need to parse
    ErrorLocation loc = emitter.getBaseLocation();
    if (exception.inputScope() != null) {
      String[] path = StringUtils.split(exception.inputScope().toString(), "/");
      for (String p : path) {
        if (!p.isBlank()) {
          loc = loc.resolve(p);
        }
      }
    }
    return new ErrorMessage.Implementation(msg, loc, ErrorMessage.Severity.FATAL);
  }
}
