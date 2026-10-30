/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.compile.check;

import com.datasqrl.compile.check.SymbolIndex.ImportLink;
import com.datasqrl.compile.check.SymbolIndex.Reference;
import com.datasqrl.compile.check.SymbolIndex.SourceRange;
import com.datasqrl.error.ErrorLocation.FileLocation;
import com.datasqrl.error.ErrorLocation.FileRange;
import com.datasqrl.packager.SourceFiles;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import org.apache.flink.table.catalog.ObjectIdentifier;

/**
 * Maps positions in the build files that the planner reads to ranges in the source files they were
 * created from. The positions are only mapped if the text at them is the name that is expected
 * there. A build file that was rendered from a template has the filled-in values, so its positions
 * are mapped to the template, see {@link RenderedText}.
 */
final class BuildPositions {

  /** A position in a build file, with the name to look for at or after it. */
  record BuildLocation(Path file, FileLocation location, String name) {}

  record PendingReference(
      Path file,
      FileLocation start,
      FileLocation end,
      ObjectIdentifier table,
      List<String> column,
      Optional<String> type) {}

  /**
   * The name of a table in a statement, outside of a query.
   *
   * @param start where the name starts
   * @param parts how many parts the name has, e.g. 2 for {@code sources.Orders}
   */
  record PendingName(Path file, FileLocation start, int parts, ObjectIdentifier table) {}

  /** An IMPORT statement, starting at the given location, and what it imports. */
  record PendingImport(Path file, FileLocation statement, BuildLocation target) {}

  /** A build file, the source file it was created from, and how their positions relate */
  private record Texts(ScriptText build, ScriptText source, RenderedText rendered) {}

  private static final Pattern ALIAS =
      Pattern.compile("\\s+AS\\s+([A-Za-z_][\\w$]*|`[^`]+`)", Pattern.CASE_INSENSITIVE);

  private final SourceFiles sourceFiles;
  private final Map<Path, Optional<Texts>> texts = new HashMap<>();

  BuildPositions(SourceFiles sourceFiles) {
    this.sourceFiles = sourceFiles;
  }

  /** The range of the name of a definition, or its start if the name is not found. */
  Optional<SourceRange> range(BuildLocation location) {
    var source = sourceFiles.getSourceFile(location.file());
    var text = text(location.file());
    if (source.isEmpty() || text.isEmpty()) {
      return Optional.empty();
    }
    var script = text.get().build();
    var start = offset(script, location.location());
    if (!location.name().isEmpty()) {
      var name = script.find(location.name(), start);
      if (name.isPresent()) {
        var parts = name.get();
        return Optional.of(
            range(
                source.get(), text.get(), parts.get(0).start(), parts.get(parts.size() - 1).end()));
      }
    }
    return Optional.of(range(source.get(), text.get(), start, start));
  }

  /**
   * Maps a reference, if the text at its position is the name it refers to. That is not the case
   * for columns expanded from {@code *}, which have the position of the {@code *}.
   */
  Optional<Reference> reference(PendingReference reference) {
    var source = sourceFiles.getSourceFile(reference.file());
    var text = text(reference.file());
    if (source.isEmpty() || text.isEmpty()) {
      return Optional.empty();
    }
    var script = text.get().build();
    var start = offset(script, reference.start());
    // The end position of the parser is inclusive
    var end = offset(script, reference.end()) + 1;
    if (end <= start || end > script.text().length()) {
      return Optional.empty();
    }
    var parts = ScriptText.parseName(script.text().substring(start, end));
    var expected =
        reference.column().isEmpty()
            ? List.of(reference.table().getObjectName())
            : reference.column();
    if (parts.isEmpty() || !ScriptText.endsWith(parts.get(), expected)) {
      return Optional.empty();
    }
    if (!reference.column().isEmpty()) {
      // Only the column part of a qualified column, e.g. `amount` of `o.amount`
      start += parts.get().get(parts.get().size() - expected.size()).start();
    }
    return Optional.of(
        new Reference(
            range(source.get(), text.get(), start, end),
            reference.table(),
            reference.column(),
            reference.type()));
  }

  /** Maps the name of a table in a statement, if the text at its location is that name. */
  Optional<Reference> name(PendingName name) {
    var source = sourceFiles.getSourceFile(name.file());
    var text = text(name.file());
    if (source.isEmpty() || text.isEmpty() || name.parts() < 1) {
      return Optional.empty();
    }
    var script = text.get().build();
    var parts = script.nameStartingAt(offset(script, name.start()));
    if (parts.size() < name.parts()
        || !parts.get(name.parts() - 1).name().equalsIgnoreCase(name.table().getObjectName())) {
      return Optional.empty();
    }
    return Optional.of(
        new Reference(
            range(
                source.get(), text.get(), parts.get(0).start(), parts.get(name.parts() - 1).end()),
            name.table(),
            List.of(),
            Optional.empty()));
  }

  /**
   * Maps the path of an IMPORT statement to what it imports, and also the alias of an imported
   * script, which names the database of its tables, e.g. {@code ctl} of {@code IMPORT pkg.script AS
   * ctl}.
   */
  List<ImportLink> importLinks(PendingImport link) {
    var source = sourceFiles.getSourceFile(link.file());
    var text = text(link.file());
    var target = range(link.target());
    if (source.isEmpty() || text.isEmpty() || target.isEmpty()) {
      return List.of();
    }
    var script = text.get().build();
    var keyword = script.find("IMPORT", offset(script, link.statement()));
    if (keyword.isEmpty()) {
      return List.of();
    }
    var start = keyword.get().get(0).end();
    while (start < script.text().length() && Character.isWhitespace(script.text().charAt(start))) {
      start++;
    }
    var path = script.pathStartingAt(start);
    if (path.isEmpty()) {
      return List.of();
    }
    var end = path.get(path.size() - 1).end();
    if (script.text().startsWith(".*", end)) {
      end += 2; // all objects of the imported script
    }
    var links = new ArrayList<ImportLink>();
    links.add(new ImportLink(range(source.get(), text.get(), start, end), target.get()));
    var alias = ALIAS.matcher(script.text()).region(end, script.text().length());
    if (link.target().name().isEmpty() && alias.lookingAt()) {
      links.add(
          new ImportLink(
              range(source.get(), text.get(), alias.start(1), alias.end(1)), target.get()));
    }
    return links;
  }

  /** Maps a range in a build file, e.g. of an error, to its source file. */
  FileRange toSource(Path buildFile, FileRange range) {
    var text = text(buildFile);
    if (text.isEmpty() || text.get().build().text().equals(text.get().source().text())) {
      return range;
    }
    var from = sourceOffset(text.get(), new FileLocation(range.fromLine(), range.fromOffset()));
    var to = sourceOffset(text.get(), new FileLocation(range.toLine(), range.toOffset()));
    var source = text.get().source();
    return new FileRange(
        source.lineOf(from) + 1,
        source.characterOf(from) + 1,
        source.lineOf(to) + 1,
        source.characterOf(to) + 1);
  }

  private static int sourceOffset(Texts text, FileLocation location) {
    return text.rendered().toSource(offset(text.build(), location));
  }

  private Optional<Texts> text(Path file) {
    return texts.computeIfAbsent(
        file,
        f -> {
          try {
            var build = Files.readString(f);
            var sourceFile = sourceFiles.getSourceFile(f);
            var source = sourceFile.isPresent() ? sourceFiles.read(sourceFile.get()) : build;
            return Optional.of(
                new Texts(
                    new ScriptText(build), new ScriptText(source), RenderedText.of(source, build)));
          } catch (IOException e) {
            return Optional.empty();
          }
        });
  }

  private static int offset(ScriptText script, FileLocation location) {
    return script.offset(location.line() - 1, location.offset() - 1);
  }

  private static SourceRange range(Path file, Texts text, int start, int end) {
    var source = text.source();
    var from = text.rendered().toSource(start);
    var to = text.rendered().toSource(end);
    return new SourceRange(
        file,
        source.lineOf(from),
        source.characterOf(from),
        source.lineOf(to),
        source.characterOf(to));
  }
}
