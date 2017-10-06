#!/bin/bash
source activate airflow
export DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export AIRFLOW_HOME=$DIR
export AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# https://stackoverflow.com/questions/9090683/supervisord-stopping-child-processes
trap "kill -- -$$" EXIT

if [[ "$1" = start ]]; then
    echo starting
    if [[ ! -e "$DIR/.airflow.initdb.touch" ]]; then
        airflow initdb
        touch $DIR/.airflow.initdb.touch
    fi
    rm -f $DIR/scheduler.og
    rm -f $DIR/webserver.og
    airflow scheduler > $DIR/scheduler.log 2>&1 &
    airflow webserver > $DIR/webserver.log 2>&1 &
    tail -f $DIR/*.log
fi
