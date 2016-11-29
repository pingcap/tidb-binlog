#!/bin/bash

CP_ROOT=$(dirname "${BASH_SOURCE}")/..

HOST="127.0.0.1"
PORT=4000
USERNAME="root"
PASSWORD="''"
DATADIR="."
CHUNKSIZE=64
CISTERN_ADDR="127.0.0.1:8249"

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
    echo_info  "outputdir: ${DATADIR}"
    echo_info  "chunk-filesize: ${CHUNKSIZE}"
    echo_info  "cistern-addr: ${CISTERN_ADDR}"
    echo_info  "##################################"
}

# parse arguments
while [[ $# -gt 1 ]]; do
    arg="$1"
    # if $2 is with prefix -, we should echo error and exit
    if [[ "$2" =~ ^\- ]]; then
        echo_error "$arg should be follow with it's argument value, not $2" && exit 1
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
    -o|--outputdir)
        DATADIR="$2"
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


TMP_DUMP_DIR="${DATADIR}/tmp_dump_files"
DUMP_DIR="${DATADIR}/dump_files"

# clean tmp dir
rm -rf ${TMP_DUMP_DIR} || mkdir ${TMP_DUMP_DIR}

# get the cistern's status
rc=0
curl -s "http://${CISTERN_ADDR}/status" > ${DATADIR}/.cistern_status || rc=$?
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

# filter and get latest commit TS
cat ${DATADIR}/.cistern_status | grep -Po '"Upper":\d+'| grep -Po '\d+' > ${DATADIR}/latest_commit_ts
