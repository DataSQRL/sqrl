package ai.dataeng.sqml;

import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.config.provider.EnvironmentPersistenceProvider;
import ai.dataeng.sqml.config.util.NamedIdentifier;
import com.google.common.base.Preconditions;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class MetadataEnvironmentPersistence implements EnvironmentPersistence {

    public static final String STORE_SUBMISSIONS_KEY = "submissions";
    public static final String STORE_DEFINITION_KEY = "submissions";

    private final MetadataStore store;


    @Override
    public Stream<ScriptDeployment> getAllDeployments() {
        return store.getSubKeys(STORE_SUBMISSIONS_KEY).stream().map(submitId -> {
            ScriptDeployment submission = store.get(ScriptDeployment.class, STORE_SUBMISSIONS_KEY, submitId,STORE_DEFINITION_KEY);
            Preconditions.checkArgument(submission!=null,
                    "Persistence of submission failed.");
            return submission;
        });
    }

    @Override
    public void saveDeployment(ScriptDeployment submission) {
        store.put(submission, STORE_SUBMISSIONS_KEY, submission.getId().getId(),STORE_DEFINITION_KEY);
    }

    @Override
    public ScriptDeployment getSubmissionById(NamedIdentifier submitId) {
        return store.get(ScriptDeployment.class, STORE_SUBMISSIONS_KEY, submitId.getId(), STORE_DEFINITION_KEY);
    }


    public static class Provider implements EnvironmentPersistenceProvider {

        @Override
        public EnvironmentPersistence createEnvironmentPersistence(MetadataStore metaStore) {
            return new MetadataEnvironmentPersistence(metaStore);
        }
    }
}
