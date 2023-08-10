package com.datasqrl.calcite.function.builtin;

import com.datasqrl.calcite.function.ITransformation;
import com.datasqrl.calcite.type.MyVectorType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;

public class MySimpleVector extends ScalarFunction implements ITransformation {

  /**
   * This code snippet will turn an input string into a 26-dimensional vector,
   * where each element in the vector represents the frequency count of a
   * letter in the English alphabet.
   */
  public MyVectorType eval(String input) {
    // Define a character set for which we'll count frequencies
    int size = 26; // Number of letters in the English alphabet
    double[] vector = new double[size];

    // Normalize input to lowercase for simplicity
    input = input.toLowerCase();

    for (char ch : input.toCharArray()) {
      if (ch >= 'a' && ch <= 'z') { // Check if the character is an English letter
        vector[ch - 'a']++; // Increment the corresponding position in the vector
      }
    }

    return new MyVectorType(vector, input, null);
  }

  @Override
  public SqlNode apply(String dialect, SqlOperator op, SqlParserPos pos, List<SqlNode> nodeList) {
//    switch (dialect) {
//      case "POSTGRES":
//      default:
//        return new SqrlUnresolvedFunction("NOW2")
//            .createCall(pos, nodeList);
//    }
    return null;
  }
}