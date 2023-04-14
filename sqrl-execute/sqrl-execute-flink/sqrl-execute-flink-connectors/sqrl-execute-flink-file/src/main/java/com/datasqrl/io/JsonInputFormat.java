package com.datasqrl.io;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringWriter;
import lombok.SneakyThrows;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.connector.file.src.reader.StreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

public class JsonInputFormat extends SimpleStreamFormat<String> {

  private static final long serialVersionUID = 1L;

  public static final String DEFAULT_CHARSET_NAME = "UTF-8";

  private final String charsetName;

  public JsonInputFormat() {
    this(DEFAULT_CHARSET_NAME);
  }

  public JsonInputFormat(String charsetName) {
    this.charsetName = charsetName;
  }

  @Override
  public JsonInputFormat.Reader createReader(Configuration config, FSDataInputStream stream) throws IOException {
    final BufferedReader reader =
        new BufferedReader(new InputStreamReader(stream, charsetName));
    return new JsonInputFormat.Reader(reader);
  }

  @Override
  public TypeInformation<String> getProducedType() {
    return Types.STRING;
  }

  // ------------------------------------------------------------------------

  public static final class Reader implements StreamFormat.Reader<String> {

    private final BufferedReader reader;
    private final JsonParser parser;
    private JsonToken previous;

    @SneakyThrows
    Reader(final BufferedReader reader) {
      this.reader = reader;
      this.parser = new JsonFactory()
          .createParser(reader);
    }

    @Override
    public String read() throws IOException {

      if (previous == null) {
        JsonToken token = parser.nextToken();
        if (token == JsonToken.START_ARRAY) {
          //in an array [{}, {}]
          //read next token as assure its a START_OBJECT
          Preconditions.checkState(parser.nextToken() == JsonToken.START_OBJECT, "Unknown json format.");
        } else if (token == JsonToken.START_OBJECT) {
          //no indicator separator, e.g {}{}
        }
      } else {
        if (previous == JsonToken.END_OBJECT) {
          parser.nextToken();
        }
      }
      if (parser.currentToken() == JsonToken.END_ARRAY || parser.currentToken() == null) {
        return null;
      }

      Preconditions.checkState(parser.currentToken() == JsonToken.START_OBJECT, "Malformed json:" + parser.currentToken());
      //Token should be a START_OBJECT

      StringWriter writer = new StringWriter();
      JsonGenerator generator = new JsonFactory().createGenerator(writer);
      generator.copyCurrentStructure(parser);

      //store the next token
      previous = parser.currentToken();

      generator.close();
      return writer.toString();

    }

    @Override
    public void close() throws IOException {
      reader.close();
      parser.close();
    }
  }
}
