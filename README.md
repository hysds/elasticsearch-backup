# elasticsearch-backup
Scripts for backup &amp; restore of ElasticSearch indexes

## v2
```
for i in backup/*; do ./re_index_v2.py $i id; done
```

## Nightly backup cron
0 1 * * * $HOME/elasticsearch-backup/backup_docs_s3.sh /backup/grfn-datasets s3://hysds-aria-backup/grfn-datasets > /tmp/elasticsearch_backup.log 2>&1
