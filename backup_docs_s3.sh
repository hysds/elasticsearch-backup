#!/bin/bash
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)
source $HOME/sciflo/bin/activate

BACKUP_PATH=$1
S3_PATH=$2
NOTIFICATION_EMAIL_TO=$3


function email_notify {
  SUBJECT=$1
  LOG_FILE=$2
  cat $LOG_FILE | mail  -s "(elasticsearch_backup) $SUBJECT"  -r "${NOTIFICATION_EMAIL_TO}"
}


function check_error {
  STATUS=$?
  if [ $STATUS -ne 0 ]; then
    echo "Failed to run $1." 1>&2
    email_notify "Failed to run $1" /tmp/elasticsearch_backup.log
    exit $STATUS
  fi
}

# manually backup docs
$BASE_PATH/backup_all.py $BACKUP_PATH || check_error dump_indexes

# rsync over to s3
aws s3 sync $BACKUP_PATH $S3_PATH || check_error s3_sync
