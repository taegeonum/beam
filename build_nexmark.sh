#!/bin/bash
cd sdks/java/nexmark
../../../gradlew shadowJar -x test

mvn install:install-file -Dfile=build/libs/beam-sdks-java-nexmark-2.6.0-SNAPSHOT.jar \
-DgroupId=org.apache.beam \
-DartifactId=beam-sdks-java-nexmark \
-Dversion=2.6.0-SNAPSHOT \
-Dpackaging=jar \
-DgeneratePom=true
