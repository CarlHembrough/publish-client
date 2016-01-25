#!/bin/bash


train_url="http://localhost:8084"

if [ ! -z "$1" ]
  then
    train_url="$1"
fi

#if [ ! -z "$2" ]
#  then
#    export BROWSERSTACK_URL="$2"
#fi

echo Running publish client against: ${train_url}

export JAVA_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,address=8888,server=y,suspend=n"

mvn package && \
java $JAVA_OPTS \
 -jar target/*-jar-with-dependencies.jar ${train_url}

exit $? # return the code from the last command.


