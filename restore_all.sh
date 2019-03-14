#!/bin/bash
source $HOME/sciflo/bin/activate


REPO_NAME="my_backup"
SNAPSHOT_NAME="grq"
NOTIFICATION_EMAIL_TO=aria.ops@list.jpl.nasa.gov


function email_notify {
  SUBJECT=$1
  LOG_FILE=$2
  cat $LOG_FILE | mail  -s "(elasticsearch_restore) $SUBJECT"  -r "${NOTIFICATION_EMAIL_TO}"
}


function check_error {
  STATUS=$?
  if [ $STATUS -ne 0 ]; then
    echo "Failed to run $1." 1>&2
    email_notify "Failed to run $1" /data/backups/elasticsearch_restore.log
    exit $STATUS
  fi
}

# register snapshot repository
curl -XPUT "http://localhost:9200/_snapshot/${REPO_NAME}" -d '{
  "type": "fs",
  "settings": {
    "location": "/data/backups/elasticsearch_snapshots",
    "compress": true
  }
}' || check_error snapshot_registration

# restore all indices
curl -XPOST "http://localhost:9200/_snapshot/${REPO_NAME}/${SNAPSHOT_NAME}/_restore" || check_error snapshot_restore
