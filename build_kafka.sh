#!/bin/bash
cd sdks/java/io/kafka
../../../../gradlew build shadowJar -x test

mvn install:install-file -Dfile=build/libs/beam-sdks-java-io-kafka-2.6.0-SNAPSHOT.jar \
-DgroupId=org.apache.beam \
-DartifactId=beam-sdks-java-io-kafka \
-Dversion=2.6.0-SNAPSHOT \
-Dpackaging=jar \
-DgeneratePom=true
