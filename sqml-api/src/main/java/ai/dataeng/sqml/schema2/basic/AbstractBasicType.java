package ai.dataeng.sqml.schema2.basic;

public class AbstractBasicType implements BasicType {

    private final String name;

    AbstractBasicType(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return getName();
    }
}
