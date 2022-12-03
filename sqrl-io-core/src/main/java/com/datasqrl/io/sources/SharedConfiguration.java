package com.datasqrl.io.sources;

import com.datasqrl.config.error.ErrorCollector;
import com.datasqrl.config.util.ConfigurationUtil;
import com.datasqrl.io.formats.FormatConfiguration;
import com.datasqrl.io.impl.CanonicalizerConfiguration;
import com.datasqrl.parse.tree.name.NameCanonicalizer;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import lombok.experimental.SuperBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import java.io.Serializable;
import java.nio.charset.Charset;

@NoArgsConstructor
@Getter
@SuperBuilder
@ToString
public abstract class SharedConfiguration implements Serializable {

  public static final String DEFAULT_CHARSET = "UTF-8";

  @NonNull @NotNull
  ExternalDataType type;
  @Builder.Default
  @NonNull
  @NotNull
  @Valid
  protected CanonicalizerConfiguration canonicalizer = CanonicalizerConfiguration.system;
  @Builder.Default
  @NonNull
  @NotNull
  protected String charset = DEFAULT_CHARSET;
  @Valid
  protected FormatConfiguration format;


  @JsonIgnore
  public NameCanonicalizer getNameCanonicalizer() {
    return canonicalizer.getCanonicalizer();
  }

  @JsonIgnore
  public Charset getCharsetObject() {
    return Charset.forName(charset);
  }


  public boolean rootInitialize(ErrorCollector errors) {
    if (!ConfigurationUtil.javaxValidate(this, errors)) {
      return false;
    }
    try {
      Charset cs = Charset.forName(charset);
    } catch (Exception e) {
      errors.fatal("Unsupported charset: %s", charset);
      return false;
    }
    if (format == null) {
      return true;
    } else {
      return format.initialize(null, errors.resolve("format"));
    }
  }

}
