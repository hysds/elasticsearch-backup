#!/usr/bin/env python
import os, sys, json, requests, argparse
import elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError, ElasticsearchException

from hysds.es_util import get_mozart_es, get_grq_es


def restore(component, backup_dir, id_key='id'):
    """Restore ES index from backup docs and mapping."""

    # get ES object
    es = get_mozart_es() if component == 'mozart' else get_grq_es()

    # get files
    idx = os.path.basename(backup_dir)
    docs_file = os.path.join(backup_dir, '%s.docs' % idx)
    if not os.path.isfile(docs_file):
        raise RuntimeError("Failed to find docs file %s" % docs_file)
    mapping_file = os.path.join(backup_dir, '%s.mapping' % idx)
    if not os.path.isfile(mapping_file):
        raise RuntimeError("Failed to find mapping file %s" % mapping_file)
    settings_file = os.path.join(backup_dir, '%s.settings' % idx)
    if not os.path.isfile(settings_file):
        raise RuntimeError("Failed to find settings file %s" % settings_file)

    # put mapping and settings
    with open(mapping_file) as f:
        mappings = json.load(f)[idx]['mappings']
    with open(settings_file) as f:
        settings = json.load(f)[idx]['settings']

    # create index
    c = elasticsearch.client.IndicesClient(es.es)
    c.create(idx, body={'settings': settings, 'mappings': mappings}, ignore=400)

    # import docs
    with open(docs_file) as f:
        for l in f:
            j = json.loads(l)
            es.index_document(index=idx, body=j, id=j[id_key])


def main():
    parser = argparse.ArgumentParser(description="Restore ElasticSearch index.")
    parser.add_argument("component", choices=['mozart', 'grq'])
    parser.add_argument("directory", help="backup directory location")
    parser.add_argument('--id_key', dest='id_key', default='id',
                        help="ID key")
    args = parser.parse_args()
    restore(args.component, args.directory, args.id_key)


if __name__ == "__main__":
    main()
