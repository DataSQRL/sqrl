/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.name;

import com.datasqrl.util.AbstractPath;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import lombok.NonNull;

public final class NamePath extends AbstractPath<Name, NamePath> {

  public static final NamePath ROOT = new NamePath();
  private static final Constructor CONSTRUCTOR = new Constructor();

  private NamePath(@NonNull Name... names) {
    super(names);
  }

  @Override
  protected Constructor constructor() {
    return CONSTRUCTOR;
  }

  public String getDisplay() {
    return Arrays.stream(elements)
        .map(e -> e.getDisplay())
        .collect(Collectors.joining("."));
  }

  public Name[] getNames() {
    return elements;
  }

  private static final class Constructor extends AbstractPath.Constructor<Name, NamePath> {

    @Override
    protected NamePath create(@NonNull Name... elements) {
      return new NamePath(elements);
    }

    @Override
    protected Name[] createArray(int length) {
      return new Name[length];
    }

    @Override
    protected NamePath root() {
      return ROOT;
    }

  }

  public static NamePath of(@NonNull Name... names) {
    return new NamePath(names);
  }

  public static NamePath of(@NonNull List<Name> names) {
    return CONSTRUCTOR.of(names);
  }

  public static NamePath of(@NonNull String... names) {
    return CONSTRUCTOR.of(Name::system, names);
  }

  public static NamePath parse(String path) {
    return CONSTRUCTOR.parse(path, s -> Name.of(s, NameCanonicalizer.SYSTEM));
  }

  public static class NamePathDeserializer extends JsonDeserializer<NamePath> {

    @Override
    public NamePath deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
        throws IOException, JacksonException {
      String namePathString = jsonParser.getValueAsString();

      if (namePathString != null && !namePathString.isEmpty()) {
        return NamePath.parse(namePathString);
      } else {
        return null;
      }
    }
  }

  public static class NamePathSerializer extends JsonSerializer<NamePath> {

    @Override
    public void serialize(NamePath namePath, JsonGenerator jsonGenerator, SerializerProvider serializerProvider)
        throws IOException {
      if (namePath != null) {
        jsonGenerator.writeString(namePath.getDisplay());
      } else {
        jsonGenerator.writeNull();
      }
    }
  }
}
