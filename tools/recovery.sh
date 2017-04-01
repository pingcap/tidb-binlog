#!/bin/bash

CP_ROOT=$(dirname "${BASH_SOURCE}")/..
LockFile=".lock.file"
HOST="127.0.0.1"
PORT=3306
USERNAME="root"
PASSWORD="''"
DUMPDIR="."
DRAINERDIR="data.drainer"
THREADS=8

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
    echo_info  "dump-files-dir: ${DUMPDIR}"
    echo_info  "drainer-savepoint: ${DRAINERDIR}"
    echo_info  "threads: ${THREADS}"
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
    -d|--dump-dir)
        DUMPDIR="$2"
        shift # past argument
        ;;
    -m|--drainer-meta)
        DRAINERDIR="$2"
        shift # past argument
        ;;
    -t|--threads)
        THREADS="$2"
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
DUMP_DIR="${DUMPDIR}/dump_files"

# backup tidb
rc=0
${CP_ROOT}/bin/loader -h ${HOST} -P ${PORT} -u ${USERNAME} -p ${PASSWORD} -t ${THREADS} -q 1 -d ${DUMP_DIR} || rc=$?
if [[ "${rc}" -ne 0 ]]; then
        exit
fi

echo_info "start synchronization!"
nohup ${CP_ROOT}/bin/drainer --config=${CP_ROOT}/conf/drainer.toml --data-dir=${DRAINERDIR} >/dev/null 2>&1 &
