#!/bin/bash

CP_ROOT=$(dirname "${BASH_SOURCE}")/..
LockFile=".lock.file"
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

echo_error () {
    echo -e "${RED}$@${NC}"
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

# lock the dir
mkdir ${LockFile}
if [ $? -ne 0 ];  then
    echo_error "another same shell script is running" && exit 1
fi
trap 'rm -rf "${LockFile}"; exit $?' INT TERM EXIT

# parse arguments
while [[ $# -gt 1 ]]; do
    arg="$1"
    # if $2 is with prefix -, we should echo error and exit
    if [[ "$2" =~ ^\- ]]; then
        echo "$arg should be follow with it's argument value, not $2" && exit 1
    fi
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
        echo_error "$1=$2" && exit 1
        ;;
    esac
    shift
done

# print args
print_args

# mydumper files
DUMP_DIR="${DATADIR}/dump_files"

# backup tidb
rc=0
${CP_ROOT}/bin/loader -h ${HOST} -P ${PORT} -u ${USERNAME} -p ${PASSWORD} -t ${THREADS} -q 1 -d ${DUMP_DIR} || rc=$?
if [[ "${rc}" -ne 0 ]]; then
        exit
fi

# get init-commit-ts
INIT_TS=`cat ${DATADIR}/latest_commit_ts`

if [[ "${ISRECOVERY}" -eq 1 ]]; then
    rc=0
    curl -s "http://${CISTERN_ADDR}/status" > ${DATADIR}/.cistern_status || rc=$?
    if [[ "${rc}" -ne 0 ]]; then
        exit
    fi

    RECOVRERY_TS=`cat ${DATADIR}/.cistern_status | grep -Po '"Upper":\d+'| grep -Po '\d+'`
    if [[ "${RECOVRERY_TS}" -gt "${INIT_TS}" ]]; then
        ${CP_ROOT}/bin/drainer --config=${CP_ROOT}/conf/drainer.toml --init-commit-ts=${INIT_TS} --end-commit-ts=${RECOVRERY_TS}
    fi
    echo_info "recovery complete!"
    exit
fi

echo_info "start synchronization!"
nohup ${CP_ROOT}/bin/drainer --config=${CP_ROOT}/conf/drainer.toml --init-commit-ts=${INIT_TS} >/dev/null 2>&1 &
