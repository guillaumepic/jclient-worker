#!/usr/bin/env bash
docker run --network host -u 1000:1000 -e MAVEN_CONFIG=/tmp/maven/.m2 --rm -v $(pwd):/home -w /home maven:3-jdk-8 mvn -Duser.home=/tmp/maven --quiet compile exec:java -Dexec.mainClass=com.gpi.App -Dexec.args="--uri=mongodb://localhost:27017/?replicaSet=test&retryWrites=true --db=jclient --col=01 --usecase=4"
