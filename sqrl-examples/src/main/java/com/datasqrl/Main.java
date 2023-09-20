package com.datasqrl;

import com.datasqrl.transformation.ConferenceTransform;

public class Main {

  public static void main(String[] args) {
    ConferenceTransform.rewriteData("/Users/matthias/Data/datasets/current23/events.json", "sqrl-examples/conference/data/events.json");
  }
}