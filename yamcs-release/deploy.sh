#!/bin/sh

mvn -P yamcs-release deploy
mvn -P yamcs-release -f pom-core2.xml deploy
