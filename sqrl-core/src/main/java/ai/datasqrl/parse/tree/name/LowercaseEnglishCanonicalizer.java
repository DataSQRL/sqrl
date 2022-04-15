package ai.datasqrl.parse.tree.name;

import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.Locale;

@EqualsAndHashCode
@ToString
public class LowercaseEnglishCanonicalizer implements NameCanonicalizer {

    @Override
    public String getCanonical(String name) {
        return name.trim().toLowerCase(Locale.ENGLISH);
    }

}
