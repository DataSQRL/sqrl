package com.datasqrl.packager.repository;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Publisher;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.data.Quickstart;
import com.datasqrl.util.data.Retail;
import lombok.SneakyThrows;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

public class RepositoryTest {

    Path localRepoPath = Path.of("repotest");
    Path outputPath = Path.of("outputtest");
    SnapshotTest.Snapshot snapshot;
    ErrorCollector errors;
    LocalRepositoryImplementation localRepo;


    @BeforeEach
    public void setup(TestInfo testInfo) throws IOException {
        this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);
        Files.createDirectories(localRepoPath);
        Files.createDirectories(outputPath);
        errors = ErrorCollector.root();
        localRepo = new LocalRepositoryImplementation(localRepoPath, errors);
    }

    @AfterEach
    public void teardown() throws IOException {
        FileUtil.deleteDirectory(localRepoPath);
        FileUtil.deleteDirectory(outputPath);
    }

    @Test
    @SneakyThrows
    public void localPublishAndRetrieve() {
        Dependency dependency = new Dependency("datasqrl.examples.ecommerce", "1.0.0", "dev");
        assertTrue(localRepo.resolveDependency(dependency.getName()).isEmpty());

        assertFalse(localRepo.retrieveDependency(outputPath, dependency));

        Publisher publisher = new Publisher(errors);
        Path ecommercePkg = Retail.BASE_PATH.resolve(Retail.INSTANCE.getName());
        publisher.publish(ecommercePkg, localRepo);
        assertFalse(errors.isFatal(), errors.toString());

        assertTrue(localRepo.resolveDependency(dependency.getName()).isEmpty()); //local repos should not resolve
        assertTrue(localRepo.retrieveDependency(outputPath, dependency));

        snapshot.addContent(FileTestUtil.getAllFilesAsString(localRepoPath),"localRepo");
        snapshot.addContent(FileTestUtil.getAllFilesAsString(outputPath),"output");
        snapshot.createOrValidate();
    }

    @Test
    @Disabled
    public void publishQuickstartLocally() {
        LocalRepositoryImplementation repo = LocalRepositoryImplementation.of(errors);
        publishLocally(Quickstart.BASE_PATH.resolve("schema"), repo);
    }

    @SneakyThrows
    public void publishLocally(Path pkgPath, LocalRepositoryImplementation repo) {
        Publisher publisher = new Publisher(errors);
        Dependency dep = publisher.publish(pkgPath, repo);
        assertFalse(errors.isFatal(), errors.toString());

        assertTrue(repo.retrieveDependency(outputPath, dep));
    }

    @Test
    @SneakyThrows
    public void remoteRepoTest() {
        Dependency dependency = new Dependency("datasqrl.examples.Nutshop", "0.1.0", "dev");

        RemoteRepositoryImplementation remoteRepo = new RemoteRepositoryImplementation(RemoteRepositoryImplementation.DEFAULT_URI);
        remoteRepo.setCacheRepository(localRepo);

        Optional<Dependency> optDep = remoteRepo.resolveDependency(dependency.getName());
        assertTrue(optDep.isPresent());
        assertEquals(dependency, optDep.get());

        Path oL = outputPath.resolve("local");
        Path rL = outputPath.resolve("remote");
        Path cL = outputPath.resolve("composite");

        assertFalse(localRepo.retrieveDependency(oL, optDep.get()));
        assertTrue(remoteRepo.retrieveDependency(rL, optDep.get()));
        //above caches in local, should be able to retrieve now
        assertTrue(localRepo.retrieveDependency(oL, optDep.get()));

        CompositeRepositoryImpl compRepo = new CompositeRepositoryImpl(List.of(localRepo, remoteRepo));
        optDep = compRepo.resolveDependency(dependency.getName());
        assertTrue(optDep.isPresent());
        assertEquals(dependency, optDep.get());
        assertTrue(compRepo.retrieveDependency(cL, optDep.get()));

        snapshot.addContent(FileTestUtil.getAllFilesAsString(localRepoPath),"localRepo");
        snapshot.addContent(FileTestUtil.getAllFilesAsString(oL),"output-local");
        snapshot.addContent(FileTestUtil.getAllFilesAsString(rL),"output-remote");
        snapshot.addContent(FileTestUtil.getAllFilesAsString(cL),"output-composite");
        snapshot.createOrValidate();
    }




}
