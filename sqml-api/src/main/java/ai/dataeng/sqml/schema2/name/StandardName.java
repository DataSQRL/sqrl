package ai.dataeng.sqml.schema2.name;

import lombok.NonNull;

public class StandardName extends AbstractName {

    private String canonicalName;
    private String displayName;

    StandardName() {} //For Kryo

    StandardName(@NonNull String canonicalName, @NonNull String displayName) {
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
