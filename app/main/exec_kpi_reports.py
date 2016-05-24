from sqlalchemy import create_engine
import pandas as pd
from sqlalchemy import text
from collections import OrderedDict
from helpers import ordered_load
import yaml
import sys

class RedShiftObj:
	def __init__(self, **kwargs):
		self.schema = kwargs.get('schema', 'kpi_processed')
		self.yaml = kwargs.get('yaml', 'kpi-diandian_2_0.yaml')
		#self.table = kwargs.get('table', 'fact_dau_snapshot')
		self.db_config = "redshift+psycopg2://biadmin:pw@{cluster}.cpaytjecvzyu.us-west-2.redshift.amazonaws.com:5439/{db}".format(cluster=kwargs.get('cluster', 'kpi-diandian'), db=kwargs.get('db', 'kpi'))
		self.app_col = kwargs.get('app_col', 'app_id')
		self.engine = create_engine(self.db_config)
		self.start_date = pd.to_datetime(kwargs.get('start_date', '2014-01-01')).date()
		self.kpi_sql = """select '{yaml}' as yaml, date_trunc('{freq}', date) as date, '{freq}' as freq, split_part({app_col}, '.', 1) as game, sum(revenue_usd) as revenue, sum(is_new_user) as new_installs, count(distinct user_key) as au, count(distinct case when revenue_usd>0 then user_key else NULL end) as payers from {schema}.fact_dau_snapshot where date_trunc('{freq}', date)>=date_trunc('{freq}', '{start_date}'::date) and {app_col} not like '%%plinga%%' and {app_col} not like '%%spil%%' group by 1,2,3,4;"""
		self.retention_sql = """
		select '{yaml}' as yaml, split_part({app_col}, '.', 1) as game, '{freq}' as freq, install_date, datediff('{freq}', install_date, date) as diff, count(distinct user_key) as c from
		(select date_trunc('{freq}', a.date) as date, a.user_key, a.{app_col}, date_trunc('{freq}', u.install_date) as install_date from {schema}.fact_dau_snapshot a join {schema}.dim_user u on a.user_key=u.user_key
			where a.date<CURRENT_DATE and date_trunc('{freq}', a.date)>=date_trunc('{freq}', '{start_date}'::date) and u.install_date<=a.date and date_trunc('{freq}', u.install_date)>=date_trunc('{freq}', '{start_date}'::date) and a.{app_col} not like '%%plinga%%' and a.{app_col} not like '%%spil%%')
			where datediff('{freq}', install_date, date)<3
			group by 1,2,3,4,5;"""

	def run(self):
		freqs = ['day', 'week', 'month']
		kpi_df_list = []
		retention_df_list = []
		
		for freq in freqs:
			kpi_sql = self.kpi_sql.format(freq=freq, app_col=self.app_col, schema=self.schema, start_date=self.start_date, yaml=self.yaml)
			retention_sql = self.retention_sql.format(freq=freq, app_col=self.app_col, schema=self.schema, start_date=self.start_date, yaml=self.yaml)
			print kpi_sql
			print retention_sql
			kpi_df_list.append(pd.read_sql_query(kpi_sql, self.engine))
			retention_df_list.append(pd.read_sql_query(retention_sql, self.engine))

		kpi_df = pd.concat(kpi_df_list)
		retention_df = pd.concat(retention_df_list)

		return kpi_df, retention_df

def main(test, append):
	print 'flags: ', test, append
	dest_host = 'kpi-diandian.cpaytjecvzyu.us-west-2.redshift.amazonaws.com'
	rs_config = "redshift+psycopg2://biadmin:pw@kpi-diandian.cpaytjecvzyu.us-west-2.redshift.amazonaws.com:5439/kpi"
	engine = create_engine(rs_config)
	if test:
		base = '/Users/funplus/workplace/analytics/'
	else:
		#production environment
		base = '/mnt/funplus/data-analytics/'
	sys.path.insert(0, base+'python/')
	from pandas_to_redshift import df_to_redshift

	dest_tables = ['all_games_kpi', 'all_games_retention']
	date_cols = ['date', 'install_date']
	yaml_base = base + 'game_config/'
	yamls = ['kpi-funplus_2_0.yaml', 'kpi-diandian_2_0.yaml', 'dotarena_2_0.yaml', 'gz_2_0.yaml', 'xy_2_0.yaml']

	kpi_df_list = []
	retention_df_list = []
	freqs = ['day', 'week', 'month']
	for fn in yamls:
		#check latest day in database - we will overwrite most recent 35 days everyday
		start_date = '2014-01-01'
		if append:
			try:
				sql = "select dateadd('day', -35, max(date)) as date from kpi_processed.all_games_kpi where yaml='{0}';"
				print sql.format(fn)
				df = pd.read_sql_query(sql.format(fn), engine)
				if len(df)>0:
					print df.to_string()
					if df['date'].values[0] is not None:
						start_date = pd.to_datetime(df['date'].values[0]).date()
						print start_date
						#delete relevant records first
						sql = "delete from kpi_processed.{tbl} where date_trunc('{freq}', {date_col})>=date_trunc('{freq}', '{sd}'::date) and yaml='{yaml}' and freq='{freq}';"
						with engine.begin() as conn:
							for freq in freqs:
								for tbl, date_col in zip(dest_tables, date_cols):
									print sql.format(tbl=tbl, date_col=date_col, yaml=fn, sd=start_date, freq=freq)
									conn.execute(text(sql.format(tbl=tbl, date_col=date_col, yaml=fn, sd=start_date, freq=freq)).execution_options(autocommit=True))
			except:
				#if there's an error we will rebuild the whole thing
				print sys.exc_info()[1]
		print start_date

		with open(yaml_base+fn, 'r') as f:
			config = list(ordered_load(f, yaml.SafeLoader))[0]
			rs_config = None
			for key in ['redshift', 'kpiredshift']:
				if key in config:
					rs_config = config[key]
					if key=='redshift':
						app_col = 'app_id'
						schema = 'kpi_processed'
					else:
						app_col = 'app'
						schema = 'processed'
					break
			if rs_config is None:
				raise "Error reading yaml config file! No config for redshift."
			#print rs_config
			r = RedShiftObj(cluster=rs_config['db_cluster'], db=rs_config['db_name'], start_date=start_date, yaml=fn, app_col=app_col, schema=schema)
			kd, rd = r.run()
			kpi_df_list.append(kd)
			retention_df_list.append(rd)

	kpi_df = pd.concat(kpi_df_list)
	retention_df = pd.concat(retention_df_list)
	df_to_redshift(kpi_df, 'kpi_processed.'+dest_tables[0], dest_host, 'kpi', append=append)
	df_to_redshift(retention_df, 'kpi_processed.'+dest_tables[1], dest_host, 'kpi', append=append)

if __name__ == '__main__':
	#usage: python exec_kpi_reports.py [test rebuild]
	#test flag is for local environment
	#rebuild to recreate all tables
	#relies on pandas_to_redshift script (dynamic import of path)
	test_flag = False
	append_flag = True
	if len(sys.argv)>1:
		if 'test' in sys.argv[1:]:
			test_flag = True
		if 'rebuild' in sys.argv[1:]:
			append_flag = False
	main(test_flag, append_flag)