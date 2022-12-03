package com.datasqrl.util;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

@Getter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class StringNamedId implements NamedIdentifier {

  private String id;

  public static StringNamedId of(@NonNull String id) {
    return new StringNamedId(id.trim());
  }

  @Override
  public String toString() {
    return id;
  }


}
