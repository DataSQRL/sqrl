package com.datasqrl.packager.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.packager.Publisher;
import com.datasqrl.packager.config.Dependency;
import com.datasqrl.packager.util.Zipper;
import com.datasqrl.util.FileTestUtil;
import com.datasqrl.util.FileUtil;
import com.datasqrl.util.SnapshotTest;
import com.datasqrl.util.StringUtil;
import com.datasqrl.util.data.Quickstart;
import com.datasqrl.util.data.Retail;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import lombok.SneakyThrows;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

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
  public void testQuickstartValidatePub() {
    Publication pub = testValidatePublication(Quickstart.INSTANCE.getDataPackageDirectory());
    assertEquals("datasqrl.seedshop", pub.getName());
    assertEquals("dev", pub.getVariant());
    assertEquals(true, pub.getLatest());
  }

  @Test
  public void testRetailValidatePub() {
    Publication pub = testValidatePublication(Retail.INSTANCE.getDataPackageDirectory());
    assertEquals("datasqrl.examples.ecommerce", pub.getName());
    assertEquals("dev", pub.getVariant());
    assertEquals(true, pub.getLatest());
    assertEquals("Simple e-commerce dataset", pub.getDescription());
    assertEquals("datasqrl", pub.getAuthorId());
  }


  @SneakyThrows
  public Publication testValidatePublication(Path packagePath) {
    ValidatePublication validate = new ValidatePublication("datasqrl", outputPath, errors);
    Publisher publisher = new Publisher(errors);
    assertNotNull(publisher.publish(packagePath, validate));
    String uid = Files.find(outputPath, 1, (p, a) -> p.getFileName().toString().endsWith("zip"))
        .map(p -> p.getFileName().toString())
        .map(s -> StringUtil.removeFromEnd(s, Zipper.ZIP_EXTENSION)).findAny().get();
    Path pubFile = Files.find(outputPath, 1, (p, a) -> p.getFileName().toString().endsWith("json"))
        .findFirst().get();
    Publication pub = new Deserializer().mapJsonFile(pubFile, Publication.class);
    assertEquals(pub.getUniqueId(), uid);
    assertTrue(Instant.now().compareTo(Instant.parse(pub.getSubmissionTime())) > 0);
    return pub;
  }


  @Test
  @SneakyThrows
  public void localPublishAndRetrieve() {
    Dependency dependency = new Dependency("datasqrl.examples.ecommerce", "1.0.0", "dev");
    assertTrue(localRepo.resolveDependency(dependency.getName()).isEmpty());

    assertFalse(localRepo.retrieveDependency(outputPath, dependency));

    Publisher publisher = new Publisher(errors);
    Path ecommercePkg = Retail.INSTANCE.getDataPackageDirectory();
    publisher.publish(ecommercePkg, localRepo);
    assertFalse(errors.isFatal(), errors.toString());

    assertTrue(localRepo.resolveDependency(dependency.getName())
        .isEmpty()); //local repos should not resolve
    assertTrue(localRepo.retrieveDependency(outputPath, dependency));

    snapshot.addContent(FileTestUtil.getAllFilesAsString(localRepoPath), "localRepo");
    snapshot.addContent(FileTestUtil.getAllFilesAsString(outputPath), "output");
    snapshot.createOrValidate();
  }

  @Test
  @Disabled
  public void publishQuickstartLocally() {
    LocalRepositoryImplementation repo = LocalRepositoryImplementation.of(errors);
    publishLocally(Quickstart.INSTANCE.getDataPackageDirectory(), repo);
  }

  @SneakyThrows
  public void publishLocally(Path pkgPath, LocalRepositoryImplementation repo) {
    Publisher publisher = new Publisher(errors);
    Dependency dep = publisher.publish(pkgPath, repo).asDependency();
    assertFalse(errors.isFatal(), errors.toString());

    assertTrue(repo.retrieveDependency(outputPath, dep));
  }

  @Test
  @SneakyThrows
  public void remoteRepoTest() {
    Dependency dependency = new Dependency("datasqrl.seedshop", "0.1.1", "dev");

    RemoteRepositoryImplementation remoteRepo = new RemoteRepositoryImplementation(
        RemoteRepositoryImplementation.DEFAULT_URI);
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

    snapshot.addContent(FileTestUtil.getAllFilesAsString(localRepoPath), "localRepo");
    snapshot.addContent(FileTestUtil.getAllFilesAsString(oL), "output-local");
    snapshot.addContent(FileTestUtil.getAllFilesAsString(rL), "output-remote");
    snapshot.addContent(FileTestUtil.getAllFilesAsString(cL), "output-composite");
    snapshot.createOrValidate();
  }


}
