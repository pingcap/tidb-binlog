#!/bin/bash

CP_ROOT=$(dirname "${BASH_SOURCE}")/..
LockFile=".lock.file"
HOST="127.0.0.1"
PORT=4000
USERNAME="root"
PASSWORD="''"
DUMPDIR="."
DRAINERDIR="data.drainer"
CHUNKSIZE=64
PD_ADDR="127.0.0.1:2379"


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
    echo_info  "chunk-filesize: ${CHUNKSIZE}"
    echo_info  "pd-addr: ${PD_ADDR}"
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
        echo_error "$arg should be follow with it's argument value, not $2" && exit 1
    fi
    case $arg in
    -pd|--pd-addr)
        PD_ADDR="$2"
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
    -d|--dump-dir)
        DUMPDIR="$2"
        shift # past argument
        ;;
    -m|--drainer-meta)
        DRAINERDIR="$2"
        shift # past argument
        ;;
    -F|--chunk-filesize)
        CHUNKSIZE="$2"
        shift # past argument
        ;;
    *)
        # unknown option
        echo_error "$1=$2" && exit 1
        ;;
    esac
    shift
done

# primt_args
print_args


TMP_DUMP_DIR="${DUMPDIR}/tmp_dump_files"
DUMP_DIR="${DUMPDIR}/dump_files"

# clean tmp dir
rm -rf ${TMP_DUMP_DIR} || mkdir ${TMP_DUMP_DIR}

# get the cistern's status
rc=0
${CP_ROOT}/bin/drainer -gen-savepoint --data-dir=${DRAINERDIR} --pd-urls=${PD_ADDR} || rc=$?
if [[ "${rc}" -ne 0 ]]; then
        rm -rf ${TMP_DUMP_DIR}
        exit
fi

# backup tidb
rc=0
${CP_ROOT}/bin/mydumper -h ${HOST} -P ${PORT} -u ${USERNAME} -p ${PASSWORD} -t 1 -F ${CHUNKSIZE} -o ${TMP_DUMP_DIR} || rc=$?
if [[ "${rc}" -ne 0 ]]; then
        rm -rf ${TMP_DUMP_DIR}
        exit
fi

# remove mysql schema dump file? how about test schema?
rm -v ${TMP_DUMP_DIR}/mysql-schema-create.sql ${TMP_DUMP_DIR}/mysql.*

# mv to specified dir
rm -rf ${DUMP_DIR} && mv ${TMP_DUMP_DIR} ${DUMP_DIR}
