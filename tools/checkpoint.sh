#!/bin/bash

CP_ROOT=$(dirname "${BASH_SOURCE}")/..

HOST="127.0.0.1"
PORT=4000
USERNAME="root"
PASSWORD="''"
DATADIR="."
CHUNKSIZE=64
CISTERN_ADDR="127.0.0.1:8249"

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
    ;;
esac
shift # past argument or value
done

TMP_DUMP_DIR="${DATADIR}/tmp_dump_files"
DUMP_DIR="${DATADIR}/dump_files"

# clean tmp dir
rm -rf ${TMP_DUMP_DIR} && mkdir ${TMP_DUMP_DIR}

# get the cistern's status
curl "http://${CISTERN_ADDR}/status" > ${DATADIR}/.cistern_status || rc=$?
if [[ "${rc}" -ne 0 ]]; then
        rm -rf ${TMP_DUMP_DIR}
        exit
fi

# backup tidb
${CP_ROOT}/bin/mydumper -h ${HOST} -P ${PORT} -u ${USERNAME} -p ${PASSWORD} -t 1 -F ${CHUNKSIZE} -o ${TMP_DUMP_DIR} || rc=$?
if [[ "${rc}" -ne 0 ]]; then
        rm -rf ${TMP_DUMP_DIR}
        exit
fi

# mv to specified dir
rm -r ${DUMP_DIR} && mv ${TMP_DUMP_DIR} ${DUMP_DIR}

# filter and get latest commit TS
cat ${DATADIR}/.cistern_status | grep -Po '"Upper":\d+'| grep -Po '\d+' > ${DATADIR}/latest_commit_ts