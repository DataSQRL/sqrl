package ai.dataeng.sqml.tree.name;

public class IdentityCanonicalizer implements NameCanonicalizer {

    @Override
    public String getCanonical(String name) {
        return name.trim();
    }

}
