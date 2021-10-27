package ai.dataeng.sqml.tree.name;

import java.util.Locale;

public class LowercaseEnglishCanonicalizer implements NameCanonicalizer {

    @Override
    public String getCanonical(String name) {
        return name.trim().toLowerCase(Locale.ENGLISH);
    }
}
