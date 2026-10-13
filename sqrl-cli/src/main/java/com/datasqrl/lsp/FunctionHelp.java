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
package com.datasqrl.lsp;

import com.datasqrl.compile.check.PlannedCatalog;
import com.datasqrl.compile.check.ScriptText;
import com.datasqrl.compile.check.ScriptText.NamePart;
import com.datasqrl.compile.check.SymbolIndex;
import com.datasqrl.compile.check.SymbolIndex.Kind;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.eclipse.lsp4j.MarkupContent;
import org.eclipse.lsp4j.MarkupKind;
import org.eclipse.lsp4j.ParameterInformation;
import org.eclipse.lsp4j.SignatureHelp;
import org.eclipse.lsp4j.SignatureInformation;
import org.eclipse.lsp4j.jsonrpc.messages.Tuple;

/**
 * The signatures of functions: of the call whose arguments are being typed, and of a function name
 * that is hovered. Table functions of scripts come from the symbol index, other functions, built-in
 * and user-defined, from the planned catalog.
 */
class FunctionHelp {

  /** The trigger characters of signature help: the start of the arguments and their separator */
  static final List<String> TRIGGERS = List.of("(", ",");

  /** The name before the parenthesis of a call */
  private static final Pattern CALLED_NAME = Pattern.compile("([\\w$`.]+)\\s*$");

  /** The maximum length of a name before the parenthesis of a call */
  private static final int MAX_NAME = 256;

  /**
   * A function call.
   *
   * @param name the qualified name of the function
   * @param argument the index of the argument at the position
   */
  record Call(List<String> name, int argument) {}

  /** A function signature with its parameters. */
  private record Signature(String name, List<String> parameters, Optional<String> documentation) {

    String label() {
      return name + "(" + String.join(", ", parameters) + ")";
    }
  }

  private final Path file;
  private final Optional<PlannedCatalog> catalog;
  private final List<SymbolIndex> indexes;

  FunctionHelp(Path file, Optional<PlannedCatalog> catalog, List<SymbolIndex> indexes) {
    this.file = file;
    this.catalog = catalog;
    this.indexes = indexes;
  }

  /**
   * The signatures of the innermost call whose arguments contain an offset.
   *
   * @param labelOffsets whether parameters can be labeled by their offsets in the signature, else
   *     they are labeled by their text
   */
  Optional<SignatureHelp> signatureHelp(String text, int offset, boolean labelOffsets) {
    // The innermost call of a function, e.g. not the parentheses of `(a + b)`
    for (var call : callsAt(text, offset)) {
      var signatures = call.name().isEmpty() ? List.<Signature>of() : signatures(call.name());
      if (!signatures.isEmpty()) {
        return Optional.of(signatureHelp(signatures, call.argument(), labelOffsets));
      }
    }
    return Optional.empty();
  }

  private static SignatureHelp signatureHelp(
      List<Signature> signatures, int argument, boolean labelOffsets) {
    var active = 0;
    while (active < signatures.size() - 1
        && signatures.get(active).parameters().size() <= argument) {
      active++;
    }
    var information = new ArrayList<SignatureInformation>();
    for (var signature : signatures) {
      var label = signature.label();
      var parameters = new ArrayList<ParameterInformation>();
      var start = signature.name().length() + 1;
      for (var parameter : signature.parameters()) {
        var info = new ParameterInformation();
        if (labelOffsets) {
          info.setLabel(Tuple.two(start, start + parameter.length()));
        } else {
          info.setLabel(parameter);
        }
        parameters.add(info);
        start += parameter.length() + 2;
      }
      var signatureInformation = new SignatureInformation(label);
      signatureInformation.setParameters(parameters);
      signature
          .documentation()
          .ifPresent(
              doc ->
                  signatureInformation.setDocumentation(
                      new MarkupContent(MarkupKind.MARKDOWN, doc)));
      information.add(signatureInformation);
    }
    var activeParameters = signatures.get(active).parameters().size();
    return new SignatureHelp(
        information, active, activeParameters == 0 ? 0 : Math.min(argument, activeParameters - 1));
  }

  /** The signatures of the function whose name is at an offset, if it is called. */
  Optional<String> hover(String text, int offset) {
    var script = new ScriptText(text);
    var name = script.nameAt(offset);
    if (name.isEmpty()) {
      return Optional.empty();
    }
    var parts = name.get();
    var after = parts.get(parts.size() - 1).end();
    while (after < text.length() && Character.isWhitespace(text.charAt(after))) {
      after++;
    }
    if (after >= text.length() || text.charAt(after) != '(') {
      return Optional.empty();
    }
    var signatures = signatures(parts.stream().map(NamePart::name).toList());
    if (signatures.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(
        signatures.stream()
            .map(Signature::label)
            .collect(Collectors.joining("\n", "```sql\n", "\n```")));
  }

  /**
   * The signatures and the class of the user-defined function that the IMPORT statement at an
   * offset imports, e.g. {@code IMPORT udfs.read_partition_sizes}.
   */
  Optional<String> importHover(String text, int offset) {
    var script = new ScriptText(text);
    var imported = FunctionSources.at(text, script.lineOf(offset), script.characterOf(offset));
    var className = imported.flatMap(i -> catalog.flatMap(c -> c.functionClass(i.name())));
    if (className.isEmpty()) {
      return Optional.empty();
    }
    var signatures = signatures(List.of(imported.get().name()));
    var labels =
        signatures.isEmpty()
            ? ""
            : signatures.stream()
                .map(Signature::label)
                .collect(Collectors.joining("\n", "```sql\n", "\n```\n\n"));
    return Optional.of(labels + "User-defined function `" + className.get() + "`");
  }

  /** The table functions of scripts with the name, or else the functions of the catalog. */
  private List<Signature> signatures(List<String> name) {
    var function = SymbolLookup.findByName(indexes, file, name).filter(s -> s.kind() != Kind.TABLE);
    if (function.isPresent()) {
      var symbol = function.get();
      return List.of(
          new Signature(
              symbol.name(),
              symbol.parameters().stream().map(p -> p.name() + " " + p.type()).toList(),
              symbol.documentation()));
    }
    var last = name.get(name.size() - 1);
    return catalog.map(c -> c.functionSignatures(last)).orElse(List.of()).stream()
        .map(parameters -> new Signature(last.toUpperCase(), parameters, Optional.empty()))
        .toList();
  }

  /**
   * The parentheses that contain an offset, innermost first, with the names before them. Comments,
   * string literals and quoted names are skipped, and hints are comments, so the arguments of hints
   * are not calls.
   */
  static List<Call> callsAt(String text, int offset) {
    // The open parentheses before the offset, with the name before each and its commas
    var open = new ArrayDeque<Call>();
    var i = 0;
    var end = Math.min(offset, text.length());
    while (i < end) {
      var c = text.charAt(i);
      if (c == '-' && text.startsWith("--", i)) {
        i = skipTo("\n", text, i + 2);
      } else if (c == '/' && text.startsWith("/*", i)) {
        i = skipTo("*/", text, i + 2);
      } else if (c == '\'' || c == '`' || c == '"') {
        i = skipQuoted(text, i);
      } else {
        if (c == '(') {
          var from = Math.max(0, i - MAX_NAME);
          var matcher = CALLED_NAME.matcher(text.substring(from, i));
          var name =
              matcher.find()
                  ? ScriptText.parseName(matcher.group(1))
                      .map(parts -> parts.stream().map(NamePart::name).toList())
                      .orElse(List.<String>of())
                  : List.<String>of();
          open.push(new Call(name, 0));
        } else if (c == ')') {
          open.poll();
        } else if (c == ',' && !open.isEmpty()) {
          var call = open.pop();
          open.push(new Call(call.name(), call.argument() + 1));
        } else if (c == ';') {
          open.clear();
        }
        i++;
      }
    }
    if (i > end) {
      return List.of(); // in a comment or quoted text
    }
    return List.copyOf(open);
  }

  private static int skipTo(String terminator, String text, int from) {
    var at = text.indexOf(terminator, from);
    return at < 0 ? Integer.MAX_VALUE : at + terminator.length();
  }

  private static int skipQuoted(String text, int start) {
    var quote = text.charAt(start);
    var i = start + 1;
    while (i < text.length()) {
      if (text.charAt(i) == quote) {
        if (i + 1 < text.length() && text.charAt(i + 1) == quote) {
          i += 2; // an escaped quote
          continue;
        }
        return i + 1;
      }
      i++;
    }
    return Integer.MAX_VALUE;
  }
}
