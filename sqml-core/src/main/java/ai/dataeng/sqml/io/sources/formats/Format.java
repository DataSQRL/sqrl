package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.io.sources.impl.InputPreview;
import ai.dataeng.sqml.io.sources.impl.file.FilePath;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import lombok.NonNull;
import lombok.Value;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

public interface Format<C extends FormatConfiguration> {

    Parser getParser(C configuration);

    Optional<C> getDefaultConfiguration();

    interface Parser {

        @Value
        class Result {

            private final Type type;
            private final Map<String,Object> record;
            private final Instant source_time;
            private final String errorMsg;

            public static Result error(String msg) {
                return new Result(Type.ERROR,null, null, msg);
            }

            public static Result success(Map<String,Object> record) {
                return new Result(Type.SUCCESS,record,null,null);
            }

            public static Result skip() {
                return new Result(Type.SKIP,null,null,null);
            }

            enum Type { ERROR, SKIP, SUCCESS }

        }

    }

    Optional<? extends ConfigurationInference<C>> getConfigInferer();

    interface ConfigurationInference<C extends FormatConfiguration> {

        double getConfidence();

        Optional<C> getConfiguration();

    }

    interface Writer {}



}
