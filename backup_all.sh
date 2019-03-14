#!/bin/bash
source $HOME/sciflo/bin/activate


BACKUP_PATH=$1
SNAPSHOT_NAME=$2
RSYNC_PATH=$3
NOTIFICATION_EMAIL_TO=$4

REPO_NAME="my_backup"
DUMP_PATH="${BACKUP_PATH}/elasticsearch"
SNAPSHOT_PATH="${BACKUP_PATH}/elasticsearch_snapshots"


function email_notify {
  SUBJECT=$1
  LOG_FILE=$2
  cat $LOG_FILE | mail  -s "(elasticsearch_backup) $SUBJECT"  -r "${NOTIFICATION_EMAIL_TO}"
}


function check_error {
  STATUS=$?
  if [ $STATUS -ne 0 ]; then
    echo "Failed to run $1." 1>&2
    email_notify "Failed to run $1" ${BACKUP_PATH}/elasticsearch_backup.log
    exit $STATUS
  fi
}

# create snapshot directory if it doesn't exist
if [ ! -d "$SNAPSHOT_PATH" ]; then
  mkdir -p $SNAPSHOT_PATH
  sudo chown -R elasticsearch:elasticsearch $SNAPSHOT_PATH
fi

# register snapshot repository
curl -XPUT "http://localhost:9200/_snapshot/${REPO_NAME}" -d "{
  \"type\": \"fs\",
  \"settings\": {
    \"location\": \"${SNAPSHOT_PATH}\",
    \"compress\": true
  }
}" || check_error snapshot_registration

# snapshot all indices
curl -XDELETE "http://localhost:9200/_snapshot/${REPO_NAME}/${SNAPSHOT_NAME}"
curl -XPUT "http://localhost:9200/_snapshot/${REPO_NAME}/${SNAPSHOT_NAME}?wait_for_completion=true" || check_error snapshot_creation

# manually backup docs
BASE_PATH=$(dirname "${BASH_SOURCE}")
BASE_PATH=$(cd "${BASE_PATH}"; pwd)
$BASE_PATH/backup_all.py $DUMP_PATH || check_error dump_indexes

# rsync over to pip:///data/backups
rsync -rptvzLe ssh $DUMP_PATH $SNAPSHOT_PATH $RSYNC_PATH/${SNAPSHOT_NAME}/ || check_error rsync_pip
