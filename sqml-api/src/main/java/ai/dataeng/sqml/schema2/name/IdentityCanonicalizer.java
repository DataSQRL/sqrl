package ai.dataeng.sqml.schema2.name;

public class IdentityCanonicalizer implements NameCanonicalizer {

    @Override
    public String getCanonical(String name) {
        return name.trim();
    }

}
