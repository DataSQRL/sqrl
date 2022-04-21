package ai.datasqrl.io.formats;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import lombok.NonNull;
import lombok.Value;

public interface Format<C extends FormatConfiguration> {

  Parser getParser(C configuration);

  C getDefaultConfiguration();

  interface Parser extends Serializable {

    @Value
    class Result {

      private final Type type;
      private final Map<String, Object> record;
      private final Instant sourceTime;
      private final String errorMsg;

      public static Result error(String msg) {
        return new Result(Type.ERROR, null, null, msg);
      }

      public static Result success(@NonNull Map<String, Object> record) {
        return new Result(Type.SUCCESS, record, null, null);
      }

      public static Result success(@NonNull Map<String, Object> record, @NonNull Instant time) {
        return new Result(Type.SUCCESS, record, time, null);
      }

      public static Result skip() {
        return new Result(Type.SKIP, null, null, null);
      }

      public boolean isSuccess() {
        return type == Type.SUCCESS;
      }

      public boolean hasTime() {
        return sourceTime != null;
      }

      public enum Type {ERROR, SKIP, SUCCESS}

    }

  }

  interface ConfigurationInference<C extends FormatConfiguration> {

    double getConfidence();

  }

  Writer getWriter(C configuration);

  interface Writer {

  }


}
