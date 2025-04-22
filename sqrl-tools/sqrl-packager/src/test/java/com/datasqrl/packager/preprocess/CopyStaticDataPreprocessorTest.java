package com.datasqrl.packager.preprocess;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

public class CopyStaticDataPreprocessorTest {

	CopyStaticDataPreprocessor underTest = new CopyStaticDataPreprocessor();

	@ParameterizedTest
	@CsvSource({ "'\\n',second line\\nthird line", "'\\r\\n',second line\\r\\nthird line",
			"'\\r',second line\\rthird line" })
	void givenTextWithMultipleLines_whenCopyFileSkipFirstLine_thenFirstLineIsRemovedAndRestPreserved(
			String escapedLineEnding, String expectedContent) throws IOException {
		// given
		var lineEnding = unescape(escapedLineEnding);
		var expected = unescape(expectedContent);
		var input = "first line" + lineEnding + expected;

		var in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
		var out = new ByteArrayOutputStream();

		// when
		underTest.copyFileSkipFirstLine(in, out);

		// then
		var result = out.toString(StandardCharsets.UTF_8);
		assertThat(result).as("Line ending: %s", repr(lineEnding)).isEqualTo(expected);
	}

	@Test
	void givenSingleLineFile_whenCopyFileSkipFirstLine_thenOutputIsEmpty() throws IOException {
		var input = "only line";
		var in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
		var out = new ByteArrayOutputStream();

		underTest.copyFileSkipFirstLine(in, out);

		assertThat(out.toString(StandardCharsets.UTF_8)).isEmpty();
	}

	@Test
	void givenEmptyFile_whenCopyFileSkipFirstLine_thenOutputIsEmpty() throws IOException {
		var in = new ByteArrayInputStream(new byte[0]);
		var out = new ByteArrayOutputStream();

		underTest.copyFileSkipFirstLine(in, out);

		assertThat(out.toString(StandardCharsets.UTF_8)).isEmpty();
	}

	@Test
	void givenLongFirstLine_whenCopyFileSkipFirstLine_thenSecondLineAndBeyondArePreserved() throws IOException {
		var lineEnding = "\n";
		var longFirstLine = "A".repeat(10_000);
		var input = longFirstLine + lineEnding + "second" + lineEnding + "third";

		var in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
		var out = new ByteArrayOutputStream();

		underTest.copyFileSkipFirstLine(in, out);

		var result = out.toString(StandardCharsets.UTF_8);
		assertThat(result).isEqualTo("second\nthird");
	}

	@Test
	void givenFirstLineIsJustLineBreak_whenCopyFileSkipFirstLine_thenAllOtherLinesRemain() throws IOException {
		var input = "\nsecond\nthird";
		var in = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8));
		var out = new ByteArrayOutputStream();

		underTest.copyFileSkipFirstLine(in, out);

		var result = out.toString(StandardCharsets.UTF_8);
		assertThat(result).isEqualTo("second\nthird");
	}

	// Helper method to visualize line endings in assertion messages
	private String repr(String s) {
		return s.replace("\r", "\\r").replace("\n", "\\n");
	}

	// Converts escaped line endings (from CSV) to actual control characters
	private String unescape(String s) {
		return s.replace("\\r", "\r").replace("\\n", "\n");
	}
}
