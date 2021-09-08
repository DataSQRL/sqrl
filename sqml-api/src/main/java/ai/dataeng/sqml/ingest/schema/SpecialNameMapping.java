package ai.dataeng.sqml.ingest.schema;

import ai.dataeng.sqml.ingest.schema.version.Version;
import ai.dataeng.sqml.schema2.name.Name;
import lombok.Value;

public interface SpecialNameMapping extends NameMapping {

    boolean isPrevious();

    boolean isRemoved();

    @Value
    public static class Previous implements SpecialNameMapping {

        public final Name previous;

        @Override
        public Name map(Version sourceVersion, Name sourceName) {
            return getPrevious();
        }

        public Name getPrevious() {
            return previous;
        }

        @Override
        public boolean isPrevious() {
            return true;
        }

        @Override
        public boolean isRemoved() {
            return false;
        }
    }

    public static SpecialNameMapping REMOVED = new SpecialNameMapping() {
        @Override
        public Name map(Version sourceVersion, Name sourceName) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isPrevious() {
            return false;
        }

        @Override
        public boolean isRemoved() {
            return true;
        }
    };

}
