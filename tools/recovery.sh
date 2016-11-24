#!/bin/bash

CP_ROOT=$(dirname "${BASH_SOURCE}")/..

HOST="127.0.0.1"
PORT=3306
USERNAME="root"
PASSWORD="''"
DATADIR="."
THREADS=8
CISTERN_ADDR="127.0.0.1:8249"
ISRECOVERY=1

# echo function
echo_info () {
    echo -e "\033[0;32m$@${NC}"
}

# print args
print_args () {
    echo_info  "arguments#########################"
    echo_info  "db-host:  ${HOST}"
    echo_info  "db-port:  ${PORT}"
    echo_info  "db-user:  ${USERNAME}"
    echo_info  "db-password: ${PASSWORD}"
    echo_info  "directory: ${DATADIR}"
    echo_info  "threads: ${THREADS}"
    echo_info  "cistern-addr: ${CISTERN_ADDR}"
    echo_info  "is-recovery: ${ISRECOVERY}"
    echo_info  "##################################"
}

# parse arguments
while [[ $# -gt 1 ]]
do
arg="$1"
case $arg in
    -c|--cistern-addr)
    CISTERN_ADDR="$2"
    shift # past argument
    ;;
    -h|--host)
    HOST="$2"
    shift # past argument
    ;;
    -P|--port)
    PORT="$2"
    shift # past argument
    ;;
    -u|--user)
    USERNAME="$2"
    shift # past argument
    ;;
    -p|--password)
    PASSWORD="$2"
    shift # past argument
    ;;
    -d|--directory)
    DATADIR="$2"
    shift # past argument
    ;;
    -t|--threads)
    THREADS="$2"
    shift # past argument
    ;;
    -r|--is-recovery)
    ISRECOVERY="$2"
    shift # past argument
    ;;
    *)
    # unknown option
    ;;
esac
shift # past argument or value
done

# print args
print_args

# mydumper files
DUMP_DIR="${DATADIR}/dump_files"

# backup tidb
${CP_ROOT}/bin/myloader -h ${HOST} -P ${PORT} -u ${USERNAME} -p ${PASSWORD} -t 1 -q ${THREADS} -d ${DUMP_DIR} || rc=$?
if [[ "${rc}" -ne 0 ]]; then
        exit
fi

# get init-commit-ts
INIT_TS=`cat ${DATADIR}/latest_commit_ts`

if [[ "${ISRECOVERY}" -eq 1 ]]; then
    curl -s "http://${CISTERN_ADDR}/status" > ${DATADIR}/.cistern_status || rc=$?
    if [[ "${rc}" -ne 0 ]]; then
        exit
    fi

    RECOVRERY_TS=`cat ${DATADIR}/.cistern_status | grep -Po '"Upper":\d+'| grep -Po '\d+'`
    if [[ "${RECOVRERY_TS}" -gt "${INIT_TS}" ]]; then
        ${CP_ROOT}/bin/drainer --config-file=${CP_ROOT}/conf/drainer.toml --init-commit-ts=${INIT_TS} --recovery-stop-ts=${RECOVRERY_TS}
    fi
    echo_info "recovery complete!"
    exit
fi

echo_info "start synchronization!"
nohup ${CP_ROOT}/bin/drainer --config-file=${CP_ROOT}/conf/drainer.toml --init-commit-ts=${INIT_TS} >/dev/null 2>&1 &
