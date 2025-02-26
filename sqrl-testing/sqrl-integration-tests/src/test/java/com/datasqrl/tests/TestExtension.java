package com.datasqrl.tests;



public interface TestExtension {
  TestExtension NOOP =
      new TestExtension() {

        @Override
        public void setup() {}

        @Override
        public void teardown() {}
      };

  void setup();

  void teardown();
}
