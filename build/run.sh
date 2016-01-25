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

java -jar target/*-jar-with-dependencies.jar ${train_url}

exit $? # return the code from the last command.


