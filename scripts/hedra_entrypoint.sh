#!/usr/bin/env bash

RUNNER_MODE=${RUNNER_MODE:-local}
CONFIG_FILEPATH=${CONFIG_FILEPATH:-"/data/config.json"}
ACTIONS_FILEPATH=${ACTIONS_FILEPATH:-"/data/actions.json"}
AUTOMATE_MODE=${AUTOMATE_MODE:-"hedra"}
CODE_FILEPATH=${CODE_FILEPATH}
REPORTER_CONFIG_FILEPATH=${REPORTER_CONFIG_FILEPATH:-"/data/reporter.json"}
LOG_LEVEL=${LOG_LEVEL:-"info"}
LEADER_IP=${LEADER_IP}
BOOTSTRAP_IP=${BOOTSTRAP_IP}
LEADERS=${LEADERS:-1}
WORKER_IP=${WORKER_IP}
WORKERS=${WORKERS:-1}
AS_SERVER=${AS_SERVER:-"false"}
DISCOVERY_MODE=${DISCOVERY_MODE:-"static"}
KUBE_FILE=${KUBE_FILE:-"config"}
KUBE_CONFIG_FILEPATH=${KUBE_CONFIG_FILEPATH:="/root/.kube/"}
KUBE_CONFIG_CONTEXT=${KUBE_CONFIG_CONTEXT}
UWSGI_INI_PATH=${UWSGI_INI_PATH:-"/uwsgi/hedra_updates_uwsgi.ini"}

service nginx start

cp "/data/$KUBE_FILE" "$KUBE_CONFIG_FILEPATH"

kubectx $KUBE_CONFIG_CONTEXT

if [[ "$AUTOMATE_MODE" == "statserve" ]] ;
then
    statserve-server
else
    if [[ "$RUNNER_MODE" == "parallel" ]] || [[ "$RUNNER_MODE" == "parallel-worker" ]] ;
    then
        echo "Parallel execution is not yet supported in containerizations"
        exit 1
    fi

    HEDRA_OPTS="--runner-mode ${RUNNER_MODE}"
    if [[ "$CODE_FILEPATH" == "/data" ]] ;
    then
        HEDRA_OPTS="$HEDRA_OPTS --code-filepath ${CODE_FILEPATH}"
    else
        HEDRA_OPTS="$HEDRA_OPTS --actions-filepath ${ACTIONS_FILEPATH}"
    fi

    HEDRA_OPTS="$HEDRA_OPTS --log-level ${LOG_LEVEL}"

    if [[ "$RUNNER_MODE" == "ephemeral-leader" ]] || [[ "$RUNNER_MODE" == "job-leader" ]] ;
    then
        LEADER_OPTS="--config-filepath ${CONFIG_FILEPATH} --workers $WORKERS --bootstrap-server-ip $BOOTSTRAP_IP"

        if [[ "$DISCOVERY_MODE" != "static" ]] ;
        then
            BOOTSTRAP_OPS="--discovery-mode $DISCOVERY_MODE --kube-config-filepath $KUBE_CONFIG_FILEPATH --kube-config-context $KUBE_CONFIG_CONTEXT"
            LEADER_OPTS="$BOOTSTRAP_OPS $LEADER_OPTS"
        fi

        HEDRA_OPTS="$LEADER_OPTS $HEDRA_OPTS"
        sleep 10
    elif [[ "$RUNNER_MODE" == "ephemeral-worker" ]] || [[ "$RUNNER_MODE" == "job-worker" ]] ;
    then
        WORKER_OPTS="--leader-ips $LEADER_IP --worker-ip $WORKER_IP"
        HEDRA_OPTS="$WORKER_OPTS $HEDRA_OPTS"
        sleep 20
    elif [[ "$RUNNER_MODE" == "bootstrap-server" ]]
    then
        HEDRA_OPTS="--runner-mode $RUNNER_MODE --bootstrap-server-ip $BOOTSTRAP_IP --leaders $LEADERS"
    fi

    if [[ ${AS_SERVER} == "true" ]] ;
    then
        HEDRA_OPTS="--uwsgi-ini-path $UWSGI_INI_PATH"
        sleep 30
        hedra-server ${HEDRA_OPTS}
    else
        sleep 10
        hedra ${HEDRA_OPTS}
    fi
fi

# tail -f /dev/null