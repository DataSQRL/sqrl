package ai.dataeng.sqml.schema2.name;

public class SpecialName extends AbstractName {

    /* A string prefix that other names cannot have to ensure uniqueness in order
       to ensure that a special name is never confused with a "real" name.
       A space in front will ensure that.
     */
    public static final String UNIQUE_PREFIX = " #";

    private final String name;

    private SpecialName(String identifier) {
        this.name = UNIQUE_PREFIX + identifier;
    }

    @Override
    public String getCanonical() {
        return name;
    }

    @Override
    public String getDisplay() {
        return name;
    }

    public static Name SINGLETON = new SpecialName("singleton");
    public static Name REMOVED = new SpecialName("removed");
    public static Name LOCAL = new SpecialName("local");

}
