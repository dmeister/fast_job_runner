import json
import hashlib

def pretty_dict(d):
    """ pretty json output of a dict """
    return json.dumps(d, sort_keys=True, indent=4)

def hash_config(config):
    """ returns the md5 hash of a configuration dict """
    return hashlib.md5(json.dumps(config, sort_keys=True)).hexdigest()