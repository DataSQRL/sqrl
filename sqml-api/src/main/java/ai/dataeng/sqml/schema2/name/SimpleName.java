package ai.dataeng.sqml.schema2.name;

public class SimpleName extends AbstractName {

    private final String name;

    public SimpleName(String name) {
        this.name = validateName(name);
    }

    @Override
    public String getCanonical() {
        return name;
    }

    @Override
    public String getDisplay() {
        return name;
    }
}
