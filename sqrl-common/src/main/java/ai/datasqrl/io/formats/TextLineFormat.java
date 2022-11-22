package ai.datasqrl.io.formats;

import java.io.BufferedReader;
import java.io.IOException;
import lombok.NonNull;

public interface TextLineFormat<C extends FormatConfiguration> extends Format<C> {

  @Override
  Parser getParser(C config);

  interface Parser extends Format.Parser {

    Result parse(@NonNull String line);

  }

  interface ConfigurationInference<C extends FormatConfiguration> extends
      Format.ConfigurationInference<C> {

    void nextSegment(@NonNull BufferedReader textInput) throws IOException;

  }

  @Override
  Writer getWriter(C configuration);

  interface Writer extends Format.Writer {


  }

}
