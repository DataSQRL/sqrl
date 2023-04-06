package com.datasqrl.data;

import com.datasqrl.cmd.RootGenerateCommand;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled("only used for data generation")
public class TestDataGenerators {

  @Test
  public void testClickStream() {
    new RootGenerateCommand().getCmd().execute(new String[]{"clickstream","-n","5000","-o","clickstream/data"});
  }

  @Test
  public void testSensorIoT() {
    new RootGenerateCommand().getCmd().execute(new String[]{"sensors","-n","10000","-o","sensors/data-epoch"});
  }


}
