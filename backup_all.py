#!/usr/bin/env python
import os, sys, requests, json, types, argparse, bz2, shutil


def backup(url, backup_root):
    """Recurse over all indices at the ElasticSearch URL and backup indices."""

    # save backup root
    if os.path.isdir(backup_root):
        saved = backup_root + '.bak'
        if os.path.isdir(saved): shutil.rmtree(saved)
        shutil.move(backup_root, saved)
    os.makedirs(backup_root)

    # get all indices
    r = requests.get('%s/_aliases' % url, verify=False, headers={'content-type': 'application/json'})
    r.raise_for_status()
    res = r.json()
    #print(json.dumps(res, indent=2))
    indices = sorted(res)

    # loop over each index and save settings, mapping, and docs
    for idx in indices:
        if idx == 'geonames': continue
        print("Backup up %s..." % idx)
        d = os.path.join(backup_root, idx)
        if not os.path.isdir(d): os.makedirs(d)

        # save settings
        r = requests.get('%s/%s/_settings' % (url, idx), verify=False, headers={'content-type': 'application/json'})
        r.raise_for_status()
        settings = r.json()
        s = os.path.join(d, '%s.settings' % idx)
        with open(s, 'w') as f:
            json.dump(settings, f, indent=2, sort_keys=True)
        print("Backed up settings for %s" % idx)

        # save mapping
        r = requests.get('%s/%s/_mapping' % (url, idx), verify=False, headers={'content-type': 'application/json'})
        r.raise_for_status()
        mapping = r.json()
        m = os.path.join(d, '%s.mapping' % idx)
        with open(m, 'w') as f:
            json.dump(mapping, f, indent=2, sort_keys=True)
        print("Backed up mapping for %s" % idx)

        # save docs
        query = {
            "query": {
                "match_all": {}
            }
        }
        doc_dir = os.path.join(d, 'docs')
        if not os.path.isdir(doc_dir): os.makedirs(doc_dir)
        r = requests.post('%s/%s/_search?scroll=120m&size=100' % (url, idx), data=json.dumps(query), verify=False, headers={'content-type': 'application/json'})
        scan_result = r.json()
        #print("scan_result: {}".format(scan_result))
        count = scan_result['hits']['total']
        scroll_id = scan_result['_scroll_id']
        while True:
            r = requests.post('%s/_search/scroll?scroll=120m' % url, data=json.dumps({'scroll_id': scroll_id}), verify=False, headers={'content-type': 'application/json'})
            if r.status_code != 200:
                print("Got status code: {}\n{}".format(r.status_code, r.content))
                r.raise_for_status()
            res = r.json()
            scroll_id = res.get('_scroll_id', None)
            if scroll_id is None or len(res['hits']['hits']) == 0: break
            for hit in res['hits']['hits']:
                txt = os.path.join(doc_dir, '%s.json' % hit['_id'])
                with open(txt, 'w') as f:
                    f.write("%s\n" % json.dumps(hit['_source'], sort_keys=True, indent=4))
        #b = os.path.join(d, '%s.docs.bz2' % idx)
        #with bz2.BZ2File(b, 'w') as f:
        #    for doc in docs:
        #        f.write("%s\n" % json.dumps(doc))
        print("Backed up docs for %s" % idx)


def main():
    parser = argparse.ArgumentParser(description="Backup all ElasticSearch indexes.")
    parser.add_argument('--url', dest='url', default='http://localhost:9200',
                        help="ElasticSearch URL to backup")
    parser.add_argument('directory', help="backup directory location")
    args = parser.parse_args()
    backup(args.url, args.directory)


if __name__ == "__main__":
    main()
