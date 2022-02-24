package ai.dataeng.sqml;

import ai.dataeng.sqml.config.metadata.MetadataStore;
import ai.dataeng.sqml.config.provider.DatasetRegistryPersistenceProvider;
import ai.dataeng.sqml.config.provider.EnvironmentPersistenceProvider;
import ai.dataeng.sqml.config.util.NamedIdentifier;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistryPersistence;
import ai.dataeng.sqml.io.sources.stats.SourceTableStatistics;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@AllArgsConstructor
public class MetadataEnvironmentPersistence implements EnvironmentPersistence {

    public static final String STORE_SUBMISSIONS_KEY = "submissions";
    public static final String STORE_DEFINITION_KEY = "submissions";

    private final MetadataStore store;


    @Override
    public Stream<ScriptSubmission> getAllSubmissions() {
        return store.getSubKeys(STORE_SUBMISSIONS_KEY).stream().map(submitId -> {
            ScriptSubmission submission = store.get(ScriptSubmission.class, STORE_SUBMISSIONS_KEY, submitId,STORE_DEFINITION_KEY);
            Preconditions.checkArgument(submission!=null,
                    "Persistence of submission failed.");
            return submission;
        });
    }

    @Override
    public void saveSubmission(ScriptSubmission submission) {
        store.put(submission, STORE_SUBMISSIONS_KEY, submission.getId().getId(),STORE_DEFINITION_KEY);
    }

    @Override
    public ScriptSubmission getSubmissionById(NamedIdentifier submitId) {
        return store.get(ScriptSubmission.class, STORE_SUBMISSIONS_KEY, submitId.getId(), STORE_DEFINITION_KEY);
    }


    public static class Provider implements EnvironmentPersistenceProvider {

        @Override
        public EnvironmentPersistence createEnvironmentPersistence(MetadataStore metaStore) {
            return new MetadataEnvironmentPersistence(metaStore);
        }
    }
}
