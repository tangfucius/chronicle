from boto import s3
from boto.s3.connection import OrdinaryCallingFormat
from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import text
import sys
from os.path import dirname, abspath

#a s3RedShiftObj object represents an event in Redshift/S3, the paths to which can be configured.
#We have methods for checking row count(Redshift)/space (S3) for each date, and can unload objects from Redshift to s3 or vice versa.
class s3RedShiftObj:
	def __init__(self, **kwargs):
		self.aws_key = kwargs.get('aws_key', '')
		self.aws_secret = kwargs.get('aws_secret', '')
		self.bucket = kwargs.get('bucket', 'com.funplusgame.bidata').replace('/', '')
		self.prefix = kwargs.get('prefix', 'events_raw/ffs/')
		self.ts_name = kwargs.get('ts_name', 'ts')

		#sanity check for where arguments
		assert 'where' in kwargs
		assert 'event' in kwargs['where'].keys()
		for k, v in kwargs['where'].items():
			assert type(v) in [str, unicode]
		self.where_dict = kwargs.get('where', {})
		self.table = kwargs.get('table', 'public.events_raw')
		self.db_config = "redshift+psycopg2://biadmin:Halfquest_2014@{cluster}.cpaytjecvzyu.us-west-2.redshift.amazonaws.com:5439/{db}".format(cluster=kwargs.get('cluster', 'bicluster'), db=kwargs.get('db', 'ffs'))
		self.engine = create_engine(self.db_config)
		self.region = kwargs.get('region', 'us-west-2')
		self.detail = kwargs.get('detail', False)
		self.s3_conn = s3.connect_to_region(self.region, aws_access_key_id=self.aws_key, aws_secret_access_key=self.aws_secret,calling_format=OrdinaryCallingFormat())
		self.rs_avg = None

	def gen_where_expr(self, date):
		date = pd.to_datetime(date).date()
		bucket_path = ''
		where_list = []
		for k,v in self.where_dict.items():
		#detail=True means that s3 path looks like ...ffs/event=QuestComplete/
			if self.detail:
				bucket_path+=str(k)+"="+str(v)+'/'
			else:
				bucket_path+=str(v)+'/'
			where_list.append(str(k)+"='"+str(v)+"'")

		where_expr = ' and '.join(where_list)
		full_prefix = self.prefix+bucket_path+date.strftime('%Y/%m/%d')+'/'
		s3_path = 's3://'+self.bucket+'/'+full_prefix
		return where_expr, full_prefix, s3_path

	def s3_folder_size(self, prefix):
		bucket = self.s3_conn.get_bucket(self.bucket)
		size = 0
		count = 0

		for key in bucket.list(prefix = prefix):
			size += key.size
			count+=1

		return size, count

   	def redshift_vs_s3_date(self, date):
   		date = pd.to_datetime(date).date()
   		where_expr, full_prefix, _ = self.gen_where_expr(date)
   		sql = "select count(1) from {table} where {where} and trunc({ts_name})='{date}'".format(table=self.table, where=where_expr, ts_name = self.ts_name, date = date)
   		redshift_count = pd.read_sql_query(sql, self.engine).iloc[0]['count']
   		print full_prefix
   		sz, _ = self.s3_folder_size(full_prefix)
   		return redshift_count, sz

   	#helper method to unload. the logic for checking where we should unload builds on this method.
   	def unload_to_s3(self, date, delete_source=True, overwrite=True, vacuum=False):
   		date = pd.to_datetime(date).date()
		where_expr, full_prefix, s3_path = self.gen_where_expr(date)
		if overwrite:
			ow_flag = 'ALLOWOVERWRITE'
		else:
			ow_flag = ''

		print s3_path, ow_flag
		unload_cmd = """unload ('select * from {table} where {where} and trunc(ts) = \\'{date}\\';') to '{s3_path}' CREDENTIALS 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}' DELIMITER '^' GZIP ESCAPE {ow_flag};""".format(table=self.table, where=where_expr.replace("'", "\\'"), s3_path=s3_path, aws_key=self.aws_key, aws_secret=self.aws_secret, ow_flag=ow_flag, date=date)
		delete_sql = """delete from {table} where {where} and trunc(ts)='{date}';""".format(table=self.table, where=where_expr, date = date)
		vacuum_sql = """vacuum {table};""".format(table=self.table)
		try:
			with self.engine.begin() as conn:
				conn.execute(text(unload_cmd))
				print unload_cmd
				print 'Done unloading. {0}'.format(s3_path)
				if delete_source:
					conn.execute(text(delete_sql))
					print delete_sql
					conn.execute(text('commit;'))
			print 'Done deleting.'
		except:
			print sys.exc_info()[1]
			print 'Unload failed! Rolling back...{0}'.format(s3_path)
		if vacuum:
			with self.engine.begin() as conn:
				conn.execute(text(vacuum_sql))

	def check_b4_unload(self, date, rs_thres=100, delete_source=True, overwrite=True, vacuum=False):
		rs_count, s3_sz = self.redshift_vs_s3_date(date)
		#if thres is a percentage, calculate against average daily count
		if rs_thres>0 and rs_thres<1:
			if self.rs_avg is None:
				where_expr, _, _ = self.gen_where_expr(date)
				sql = """select avg(count) as count from (select trunc({ts_name}) as date, count(1) from {table} where {where} and {ts_name}<CURRENT_DATE group by 1);""".format(ts_name=self.ts_name, table=self.table, where=where_expr)
				rs_avg = pd.read_sql_query(sql, self.engine).iloc[0]['count']
				self.rs_avg = rs_avg
			else:
				rs_avg = self.rs_avg
			rs_thres = rs_thres*rs_avg

		if rs_count<=rs_thres:
			print "Only {0} rows in Redshift - fewer than {1} rows needed! Exiting...".format(rs_count, rs_thres)
			return
		else:
			self.unload_to_s3(date, delete_source, overwrite, vacuum)

	def copy_to_redshift(self, date, dest_table, vacuum=False):
		date = pd.to_datetime(date).date()
		where_expr, _, s3_path = self.gen_where_expr(date)
		delete_sql = """delete from {table} where {where} and trunc(ts)='{date}';""".format(table=dest_table, where=where_expr, date = date)
		copy_sql = """copy {table} from '{s3_path}' CREDENTIALS 'aws_access_key_id={aws_key};aws_secret_access_key={aws_secret}' DELIMITER '^' GZIP ESCAPE;""".format(table=dest_table, s3_path=s3_path, aws_key=self.aws_key, aws_secret=self.aws_secret)
		vacuum_sql = """vacuum {table};""".format(table=self.table)
		print delete_sql
		print copy_sql
		try:
			with self.engine.begin() as conn:
				conn.execute(text(delete_sql))
				conn.execute(text(copy_sql).execution_options(autocommit=True))
				conn.execute(text('commit;'))
			print 'Done deleting and copying. {0}'.format(s3_path)
		except:
			print sys.exc_info()[1]
			print 'Copy failed! Rolling back...{0}'.format(s3_path)
			return -99, repr(sys.exc_info()[1])
		if vacuum:
			with self.engine.begin() as conn:
				conn.execute(text(vacuum_sql))
		return 0, 'Done deleting and copying. {0}'.format(s3_path)

	#value3 should hold the original number of rows in redshift
	def sync_to_es(self, es, date, index='ffskpi', doc_type='record'):
		date = pd.to_datetime(date).date()
		allowed_kws = set(['event', 'app', 'app_id'])
		if set(self.where_dict.keys()) <= allowed_kws:
			rs_count, s3_sz = self.redshift_vs_s3_date(date)
			row = {}
			row['event'] = 'rawevents_'+self.where_dict['event']
			for k in self.where_dict.keys():
				if k.startswith('app'):
					row['app'] = self.where_dict[k]
			row['date'] = date
			row['value'] = rs_count
			row['value2'] = s3_sz
			row_id = hash(str(row['date'])+row['event']+row.get('app', ''))
			try:
				oldrow = es.get(index=index, doc_type=doc_type, id=row_id)['_source']
				oldmax = oldrow.get('value3', 0)
				row['value3'] = max(row['value'], oldmax)
			except:
				row['value3'] = row['value']
			print row
			es.index(index=index, doc_type=doc_type, id=row_id, body=row)

	def clean(self, days, es=None, rs_thres=100, delete_source=True, sync_to_es=False):
		#just generate a where_expr which we can use
		where_expr, _, s3_path = self.gen_where_expr('2016-01-01')
		sql = "select distinct trunc({ts_name}) as date from {table} where {where} and datediff('day', {ts_name}, CURRENT_DATE)>{days} order by 1".format(table=self.table, where=where_expr, ts_name = self.ts_name, days=days)
		dates = pd.read_sql_query(sql, self.engine)['date'].values
		for d in dates:
			self.check_b4_unload(d, rs_thres, delete_source)
			if sync_to_es:
				self.sync_to_es(es, d)

#test on command line
def main(test, fn):
    cfg = {'aws_key':'AKIAJ7DQBN5LXCCFQKJA',
    	   'aws_secret':'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',
    	   'where': {'event': 'Achievement'}
    	   }

    from collections import OrderedDict
    from helpers import ordered_load
    import yaml    
    with open(fn, 'r') as f:
    	clean_config = list(ordered_load(f, yaml.SafeLoader))[0]

    from elasticsearch import Elasticsearch
    if test:
    	es = Elasticsearch()
    else:
    	es = Elasticsearch('52.74.59.31:9200')

    aws_key = clean_config['aws_key']
    aws_secret = clean_config['aws_secret']
    for event, days in clean_config['events'].items():
		cfg = {'aws_key': aws_key,
	    	   'aws_secret': aws_secret,
	    	   'where': {'event': event}
	    	  }

		obj = s3RedShiftObj(**cfg)
		assert len(obj.where_dict)>0
		obj.clean(days, es=es, rs_thres=0.2, sync_to_es=True)  

if __name__ == '__main__':
	test_flag = False
	fn = '../configs/clean_config.yaml'
	if len(sys.argv)>1:
		if sys.argv[1]=='test':
			test_flag = True
		if sys.argv[-1]!='test':
			fn = sys.argv[-1]

	main(test_flag, fn)
