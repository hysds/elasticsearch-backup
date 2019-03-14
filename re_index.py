#!/usr/bin/env python
import os, sys, json, requests


def restore(backup_dir, id_key='id'):
    """Restore ES index from backup docs and mapping."""

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


    # create index
    r = requests.put('http://localhost:9200/%s' % idx)
    if r.status_code != 200:
        j = r.json()
        if r.status_code == 400 and j.get('error', '').startswith("IndexAlreadyExists"):
            pass
        else: r.raise_for_status()

    # put mapping and settings
    with open(mapping_file) as f:
        mapping = json.load(f)
    if len(mapping[idx]['mappings']) > 1:
        raise RuntimeError("More than one doctype found. Will not be able to know which to restore to.")
    with open(settings_file) as f:
        settings = json.load(f)
    doctype = None
    for dt in mapping[idx]['mappings']:
        m = mapping[idx]['mappings'][dt] 
        if idx not in settings or 'settings' not in settings.get(idx, {}):
            raise RuntimeError("Failed to find settings for index %s." % idx)
        s = settings[idx]['settings']
        r = requests.put('http://localhost:9200/%s/_mapping/%s' % (idx, dt), data=json.dumps(m))
        r = requests.put('http://localhost:9200/%s/_settings' % idx, data=json.dumps(s))
        doctype = dt 

    # import docs
    with open(docs_file) as f:
        for l in f:
            j = json.loads(l)
            r = requests.put('http://localhost:9200/%s/%s/%s' % (idx, doctype, j[id_key]), data=l)
            if r.status_code != 201:
                print(r.status_code)
                print(r.json())
                continue
            else: r.raise_for_status()


if __name__ == "__main__":
    backup_idx_dir = sys.argv[1]
    id_key = sys.argv[2]
    restore(backup_idx_dir, id_key)
