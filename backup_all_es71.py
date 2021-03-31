#!/usr/bin/env python
import os, sys, requests, json, types, argparse, bz2, shutil
import elasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError, ElasticsearchException

from hysds.es_util import get_mozart_es, get_grq_es


def backup(component, backup_root):
    """Recurse over all indices at the ElasticSearch URL and backup indices."""

    # get ES object
    es = get_mozart_es() if component == 'mozart' else get_grq_es()

    # save backup root
    if os.path.isdir(backup_root):
        saved = backup_root + ".bak"
        if os.path.isdir(saved):
            shutil.rmtree(saved)
        shutil.move(backup_root, saved)
    os.makedirs(backup_root)

    # get all indices
    c = elasticsearch.client.IndicesClient(es.es)
    indices = sorted(c.get_alias().keys())

    # loop over each index and save settings, mapping, and docs
    for idx in indices:
        if idx == "geonames":
            continue
        print("Backup up %s..." % idx)
        d = os.path.join(backup_root, idx)
        if not os.path.isdir(d):
            os.makedirs(d)

        # save settings
        settings = c.get_settings(idx)
        s = os.path.join(d, "%s.settings" % idx)
        with open(s, "w") as f:
            json.dump(settings, f, indent=2, sort_keys=True)
        print("Backed up settings for %s" % idx)

        # save mapping
        mapping = c.get_mapping(idx)
        m = os.path.join(d, "%s.mapping" % idx)
        with open(m, "w") as f:
            json.dump(mapping, f, indent=2, sort_keys=True)
        print("Backed up mapping for %s" % idx)

        # save docs
        query = {"query": {"match_all": {}}}
        txt = os.path.join(d, "%s.docs" % idx)
        with open(txt, "w") as f:
            for hit in es.query(body=query, index=idx):
                f.write("%s\n" % json.dumps(hit["_source"]))
        # b = os.path.join(d, '%s.docs.bz2' % idx)
        # with bz2.BZ2File(b, 'w') as f:
        #    for doc in docs:
        #        f.write("%s\n" % json.dumps(doc))
        print("Backed up docs for %s" % idx)


def main():
    parser = argparse.ArgumentParser(description="Backup all ElasticSearch indexes.")
    parser.add_argument("component", choices=['mozart', 'grq'])
    parser.add_argument("directory", help="backup directory location")
    args = parser.parse_args()
    backup(args.component, args.directory)


if __name__ == "__main__":
    main()
