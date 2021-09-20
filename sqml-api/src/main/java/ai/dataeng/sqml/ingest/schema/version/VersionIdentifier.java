package ai.dataeng.sqml.ingest.schema.version;

public interface VersionIdentifier {

    String getId();

    public static final VersionIdentifier BASE_VERSION_ID = new StringVersionId("0");

}
