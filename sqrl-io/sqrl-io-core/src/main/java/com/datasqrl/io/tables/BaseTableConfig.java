package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.config.Constraints.Default;
import com.datasqrl.config.Constraints.MinLength;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.impl.CanonicalizerConfiguration;
import com.google.common.base.Strings;
import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@NoArgsConstructor
@Builder(toBuilder = true)
@AllArgsConstructor
public class BaseTableConfig implements Serializable {

  public static final String DEFAULT_CANONICALIZER = CanonicalizerConfiguration.system.name();

  public static final String SCHEMA_KEY = "schema";

  String type;
  @Default
  String canonicalizer = DEFAULT_CANONICALIZER;
  @Default @Getter @MinLength(min = 1)
  String identifier = null;
  @Default @Getter
  String schema = null;

  public NameCanonicalizer getCanonicalizer() {
    return CanonicalizerConfiguration.valueOf(canonicalizer).getCanonicalizer();
  }

  public boolean hasIdentifier() {
    return !Strings.isNullOrEmpty(identifier);
  }

  public ExternalDataType getType() {
    return ExternalDataType.valueOf(type);
  }


}
