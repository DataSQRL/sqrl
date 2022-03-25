package ai.dataeng.sqml.io.sources.formats;

import ai.dataeng.sqml.io.sources.impl.file.FilePath;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import lombok.NonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Optional;
import java.util.function.Predicate;

public interface TextLineFormat<C extends FormatConfiguration> extends Format<C> {

    @Override
    Parser getParser(C config);

    interface Parser extends Format.Parser {

        public Result parse(@NonNull String line);

    }

    @Override
    Optional<ConfigurationInference<C>> getConfigInferer();

    interface ConfigurationInference<C extends FormatConfiguration> extends Format.ConfigurationInference<C> {

        void nextSegment(@NonNull BufferedReader textInput) throws IOException;

    }




    static void readUntil(FilePath file, FileSourceConfiguration fileConfig, Predicate<String> reader) throws IOException {
        try (InputStream istream = file.read();
             BufferedReader input =  new BufferedReader(new InputStreamReader(istream, fileConfig.getCharset()))) {
            while (!reader.test(input.readLine())) {}
        }
    }

}
