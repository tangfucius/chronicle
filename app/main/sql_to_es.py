from helpers import sql_to_es, ordered_load
import yaml
from elasticsearch import Elasticsearch
import sys 
from collections import OrderedDict
from sqlalchemy import create_engine, text
import sqlalchemy
from os.path import dirname, basename, abspath
import os

redshift_cfg = {'BI': {'cluster': 'bicluster.cpaytjecvzyu', 'user': 'biadmin', 'pw': 'Halfquest_2014'},
                'Tools': {'cluster': 'ffs-oplog.c2dd4vsii706', 'user': 'ffs', 'pw': 'flu89uKm0nh'}
}

def main(test, fn, **kwargs):    
    with open(fn, 'r') as f:
    	configs = list(ordered_load(f, yaml.SafeLoader))[0]['ffs']

    if test:
    	es = Elasticsearch()
    else:
    	es = Elasticsearch('52.74.59.31:9200')

    for cfg in configs:
        if 'event' in cfg:
            rs_cfg = redshift_cfg[cfg['db']]
            db_config = "redshift+psycopg2://{user}:{pw}@{cluster}.us-west-2.redshift.amazonaws.com:5439/{db}".format(cluster=rs_cfg.get('cluster', 'bicluster.cpaytjecvzyu'), user=rs_cfg.get('user', 'biadmin'), pw = rs_cfg.get('pw', 'Halfquest_2014'), db=rs_cfg.get('db', 'ffs'))
            engine = create_engine(db_config)
            try:
                sql_to_es(es, cfg['sql'], engine, event=cfg['event'])
            except:
                print 'SQL to ES error:'
                print sys.exc_info()

if __name__ == '__main__':
    test_flag = False
    fn = '/'.join(abspath(__file__).split('/')[:-2]) +'/configs/sql_to_es_config.yaml'
    if len(sys.argv)>1:
		if sys.argv[1]=='test':
			test_flag = True
		if sys.argv[-1]!='test':
			fn = sys.argv[-1]

    main(test_flag, fn)