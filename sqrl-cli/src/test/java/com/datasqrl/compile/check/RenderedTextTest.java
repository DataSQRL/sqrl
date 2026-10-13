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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class RenderedTextTest {

  @Test
  void givenFilledInValues_whenToSource_thenMapsTheTextAroundThemToTheTemplate() {
    var source = "IMPORT connectors.police_{{variant}} AS ctl;\nX := '{{a}}{{b}}' || ctl;";
    var rendered = "IMPORT connectors.police_test AS ctl;\nX := 'aaaaa' || ctl;";
    var text = RenderedText.of(source, rendered);

    // Before, after and in the value of a tag
    assertThat(text.toSource(rendered.indexOf("police"))).isEqualTo(source.indexOf("police"));
    assertThat(text.toSource(rendered.indexOf("ctl"))).isEqualTo(source.indexOf("ctl"));
    assertThat(text.toSource(rendered.indexOf("est"))).isEqualTo(source.indexOf("{{variant}}"));
    assertThat(text.toSource(rendered.indexOf(" AS"))).isEqualTo(source.indexOf(" AS"));
    // On the next line, after two values that are next to each other
    assertThat(text.toSource(rendered.lastIndexOf("ctl"))).isEqualTo(source.lastIndexOf("ctl"));
  }

  @Test
  void givenEmptyValue_whenToSource_thenMapsTheTextAfterItToTheEndOfTheTag() {
    var text = RenderedText.of("a{{missing}}b", "ab");

    assertThat(text.toSource(1)).isEqualTo("a{{missing}}".length());
  }

  @Test
  void givenSectionsOnTheirOwnLines_whenToSource_thenMapsTheRenderedOnes() {
    var source =
        """
        A := 1;
        {{#test}}
        B := {{v}};
        {{/test}}
        {{^test}}
        C := 2;
        {{/test}}
        D := 3;
        """;
    // Mustache leaves out the lines of the section tags
    var rendered = "A := 1;\nB := x;\nD := 3;\n";
    var text = RenderedText.of(source, rendered);

    assertThat(text.toSource(rendered.indexOf("B"))).isEqualTo(source.indexOf("B"));
    assertThat(text.toSource(rendered.indexOf("x"))).isEqualTo(source.indexOf("{{v}}"));
    assertThat(text.toSource(rendered.indexOf("D"))).isEqualTo(source.indexOf("D"));

    var inverted = RenderedText.of(source, "A := 1;\nC := 2;\nD := 3;\n");
    assertThat(inverted.toSource("A := 1;\n".length())).isEqualTo(source.indexOf("C"));
    assertThat(inverted.toSource("A := 1;\nC := 2;\n".length())).isEqualTo(source.indexOf("D"));
  }

  @Test
  void givenSectionsInLines_whenToSource_thenRepeatedTextMapsToTheSection() {
    var source = "D {{#list}}[{{.}}]{{/list}} E{{#none}}F{{/none}} G";
    var rendered = "D [a][b] E G";
    var text = RenderedText.of(source, rendered);

    assertThat(text.toSource(rendered.lastIndexOf("["))).isEqualTo(source.indexOf("["));
    assertThat(text.toSource(rendered.indexOf("b"))).isEqualTo(source.indexOf("{{.}}"));
    assertThat(text.toSource(rendered.indexOf("E"))).isEqualTo(source.indexOf("E"));
    assertThat(text.toSource(rendered.indexOf("G"))).isEqualTo(source.indexOf("G"));
  }

  @Test
  void givenCommentAloneOnItsLine_whenToSource_thenMapsWhetherOrNotTheLineIsRendered() {
    var source = "x\n  {{! a comment }}\ny";

    assertThat(RenderedText.of(source, "x\n  \ny").toSource(5)).isEqualTo(source.indexOf("y"));
    assertThat(RenderedText.of(source, "x\ny").toSource(2)).isEqualTo(source.indexOf("y"));
  }

  @Test
  void givenTextThatTheTemplateCannotRender_whenToSource_thenMapsNothing() {
    var text = RenderedText.of("a{{#flag}}b{{/flag}}c", "abbd");

    assertThat(text.toSource(3)).isEqualTo(3);
  }
}
