// Copyright 2017 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.cloud.sme.pubsub;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.File;
import java.io.FileWriter;

/** A basic Pub/Sub publisher for purposes of demonstrating use of the API. */
public class VerifierWriter {
  private File verifierOutput;
  private FileWriter verifierWriter;
  private BufferedWriter verifierBWriter;

  public VerifierWriter(String fileName) {
    try {
      this.verifierOutput = new File(fileName);
      this.verifierOutput.delete();
      this.verifierOutput.createNewFile();
      this.verifierWriter = new FileWriter(this.verifierOutput);
      this.verifierBWriter = new BufferedWriter(this.verifierWriter);
    } catch (IOException e) {
      System.out.println("Could not create verifier output: " + e);
      System.exit(1);
    }
  }

  public synchronized void write(String orderingKey, int sequenceNum) {
    try {
      verifierBWriter.write("\"" + orderingKey + "\",\"message" + sequenceNum + "\"");
      verifierBWriter.newLine();
    } catch (IOException e) {
      System.out.println("Failed to write published message: " + e);
      System.exit(1);
    }
  }

  public synchronized void shutdown() {
    try {
      verifierBWriter.close();
    } catch (IOException e) {
      System.out.println("Failed to close: " + e);
    }
  }
}
