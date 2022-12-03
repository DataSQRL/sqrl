package ai.datasqrl.parse.tree.name;

import java.util.Locale;
import lombok.EqualsAndHashCode;
import lombok.ToString;

@EqualsAndHashCode
@ToString
public class LowercaseEnglishCanonicalizer implements NameCanonicalizer {

  @Override
  public String getCanonical(String name) {
    return name.trim().toLowerCase(Locale.ENGLISH);
  }

}
