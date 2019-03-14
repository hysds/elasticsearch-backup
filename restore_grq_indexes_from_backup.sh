#!/bin/bash
curl -XDELETE localhost:9200/grq_*
for i in backup/grq_*; do 
  ./re_index_v2.py $i id
done
