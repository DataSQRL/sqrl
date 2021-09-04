package ai.dataeng.sqml.schema2.name;

public class StandardName extends AbstractName {

    private final String canonicalName;
    private final String displayName;

    StandardName(String canonicalName, String displayName) {
        this.canonicalName = validateName(canonicalName);
        this.displayName = validateName(displayName);
    }

    @Override
    public String getCanonical() {
        return canonicalName;
    }

    @Override
    public String getDisplay() {
        return displayName;
    }


}
