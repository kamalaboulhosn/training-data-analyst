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

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.api.gax.grpc.InstantiatingGrpcChannelProvider;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.joda.time.DateTime;
import org.threeten.bp.Duration;

/** A basic Pub/Sub subscriber for purposes of demonstrating use of the API. */
public class Subscriber implements MessageReceiver {
  public static class Args {
    @Parameter(
        names = {"--project", "-p"},
        required = true,
        description = "The Google Cloud Pub/Sub project in which the subscription exists.")
    public String project = null;

    @Parameter(
        names = {"--subscription", "-s"},
        required = true,
        description = "The Google Cloud Pub/Sub subscription name to which to subscribe.")
    public String subscription = null;
  }

  private static final String VERIFIER_OUTPUT = "received.csv";
  private static final String TIMESTAMP_KEY = "publish_time";
  private static final String ORDERING_SEQUENCE_KEY = "ordering_seq";

  private final Args args;
  private com.google.cloud.pubsub.v1.Subscriber subscriber;

  private AtomicLong receivedMessageCount = new AtomicLong(0);
  private Long lastTimestamp = new Long(0);
  private Long outOfOrderCount = new Long(0);
  private Long lastReceivedTimestamp = new Long(0);
  private ConcurrentHashMap<Integer, Integer> uniqueSeqNums = new ConcurrentHashMap<>();
  private ConcurrentHashMap<String, String> uniqueMessageIDs = new ConcurrentHashMap<>();

  private VerifierWriter vWriter;

  private Subscriber(Args args) {
    this.args = args;

    InstantiatingGrpcChannelProvider loadtestProvider =
        InstantiatingGrpcChannelProvider.newBuilder()
            .setEndpoint("loadtest-pubsub.sandbox.googleapis.com:443")
            .build();

    ProjectSubscriptionName subscription =
        ProjectSubscriptionName.of(args.project, args.subscription);
    com.google.cloud.pubsub.v1.Subscriber.Builder builder =
        com.google.cloud.pubsub.v1.Subscriber.newBuilder(subscription, this)
            .setChannelProvider(loadtestProvider)
            .setMaxAckExtensionPeriod(Duration.ofMinutes(60));
    try {
      this.subscriber = builder.build();
    } catch (Exception e) {
      System.out.println("Could not create subscriber: " + e);
      System.exit(1);
    }

    this.vWriter = new VerifierWriter(VERIFIER_OUTPUT);
  }

  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer consumer) {
    long size = message.getData().size();
    long now = DateTime.now().getMillis();
    String publishTime = message.getAttributesOrDefault(TIMESTAMP_KEY, "");
    String sequenceNumStr = message.getAttributesOrDefault(ORDERING_SEQUENCE_KEY, "-1");
    int sequenceNum = Integer.parseInt(sequenceNumStr);
    uniqueSeqNums.put(sequenceNum, sequenceNum);
    uniqueMessageIDs.put(message.getMessageId(), message.getMessageId());
    long receivedCount = receivedMessageCount.addAndGet(1);
    vWriter.write(message.getOrderingKey(), sequenceNum);
    if (publishTime != "") {
      long publishTimeParsed = 0L;
      try {
        publishTimeParsed = Long.parseLong(publishTime);
      } catch (NumberFormatException e) {
        System.out.println("Could not parse " + publishTime);
      }

      synchronized (lastTimestamp) {
        lastReceivedTimestamp = now;
        if (lastTimestamp > publishTimeParsed) {
          ++outOfOrderCount;
        } else {
          lastTimestamp = publishTimeParsed;
        }
        lastTimestamp = publishTimeParsed;
      }
    }
    if (receivedCount == 1) {
      System.out.println("First message received");
    }
    if (receivedCount % 10000 == 0) {
      System.out.println(
          "Received "
              + receivedCount
              + " messages, "
              + uniqueSeqNums.size()
              + " unique sequence numbers, "
              + uniqueMessageIDs.size()
              + " unique message IDs.");
    }
    consumer.ack();
  }

  private void run() {
    subscriber.startAsync();
    while (true) {
      long now = DateTime.now().getMillis();
      synchronized (lastTimestamp) {
        if ((now - lastReceivedTimestamp) > 60000) {
          System.out.println(
              "No message received in a minute, "
                  + receivedMessageCount.get()
                  + " messages received, "
                  + uniqueSeqNums.size()
                  + " unique sequence numbers, "
                  + uniqueMessageIDs.size()
                  + " unique message IDs.");
          vWriter.flush();
        }
        if (lastReceivedTimestamp > 0 && ((now - lastReceivedTimestamp) > 600000)) {
          subscriber.stopAsync();
          break;
        }
      }
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        System.out.println("Error while waiting for completion: " + e);
      }
    }
    System.out.println("Subscriber has not received message in 60s. Stopping.");
    subscriber.awaitTerminated();
    vWriter.shutdown();
  }

  public static void main(String[] args) {
    Args parsedArgs = new Args();
    JCommander.newBuilder().addObject(parsedArgs).build().parse(args);
    Subscriber s = new Subscriber(parsedArgs);
    s.run();
    System.exit(0);
  }
}
