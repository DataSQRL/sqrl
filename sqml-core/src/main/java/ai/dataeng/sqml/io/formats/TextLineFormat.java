package ai.dataeng.sqml.io.formats;

import lombok.NonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Optional;

public interface TextLineFormat<C extends FormatConfiguration> extends Format<C> {

    @Override
    Parser getParser(C config);

    interface Parser extends Format.Parser {

        public Result parse(@NonNull String line);

    }

    interface ConfigurationInference<C extends FormatConfiguration> extends Format.ConfigurationInference<C> {

        void nextSegment(@NonNull BufferedReader textInput) throws IOException;

    }

    @Override
    Writer getWriter(C configuration);

    interface Writer extends Format.Writer {



    }

}
