#!/bin/bash
set -m
PROJECT=`curl http://metadata.google.internal/computeMetadata/v1/project/project-id -H "Metadata-Flavor: Google"`
# Manually create the subscription ordered-subscription-test as a subscription on pubsub-e2e-example
SUBSCRIPTION=ordered-subscription-test
echo "Starting publisher and publishing 1M messages."
java -Xmx16384m -cp target/pubsub.jar com.google.cloud.sme.pubsub.Publisher -p $PROJECT
echo "Starting subscriber."
java -Xmx16384m -cp target/pubsub.jar com.google.cloud.sme.pubsub.Subscriber -p $PROJECT --subscription $SUBSCRIPTION
