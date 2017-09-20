#!/bin/bash
export DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
export AIRFLOW_HOME=$DIR

# https://stackoverflow.com/questions/9090683/supervisord-stopping-child-processes
trap "kill -- -$$" EXIT

# https://issues.apache.org/jira/browse/AIRFLOW-1102
# pip uninstall -y gunicorn
# pip install gunicorn==19.3.0
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
