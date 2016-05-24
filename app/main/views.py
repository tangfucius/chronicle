from flask import render_template, Flask, request, make_response, Response, redirect, url_for, session, flash, current_app
from flask.ext.login import login_required, current_user
from flask_bootstrap import Bootstrap
import pandas as pd
import numpy as np
import json
import yaml
from sqlalchemy import text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from helpers import *
import xml.etree.ElementTree as ET
from datetime import date
from werkzeug.contrib.cache import SimpleCache
import sys
import re
import concurrent.futures as futures
from string import lowercase
from elasticsearch import Elasticsearch
from . import main
from os.path import dirname
import pprint
from .forms import RecordForm
import html
from .s3_redshift_raw_manage import s3RedShiftObj

cache = SimpleCache()
print dirname(dirname(__file__))
with open(dirname(dirname(__file__))+'/configs/db_config.yaml', "r") as f:
	conf = yaml.safe_load(f)
pd.set_option('display.max_colwidth', -1)

db_configs = []
for name, config in conf['config'].items():
	tmp = {}
	tmp['name'] = name
	tmp['host'] = config['db_host']
	tmp['db'] = config['db_name']
	db_configs.append(tmp)
print db_configs
dangerous_words = ['delete', 'truncate', 'insert', 'drop']

#conn = "redshift+psycopg2://{user}:{pw}@{host}:{port}/{db}".format(user=conf['db_username'], pw=conf['db_password'], port=conf['db_port'], host=conf['config'][default]['db_host'], db=conf['config'][default]['db_name'])
#engine = create_engine(conn)

def retro_dictify(df):
    d = df.drop('column_name', axis=1).drop_duplicates().groupby('table_schema').apply(lambda x: x['table_name'].values).reset_index()
    d.columns = ['schema', 'table']
    return d.to_json(orient='records')

#class for query fragments
class QueryFragment:
	def __init__(self, **args):
		defaults = {'connector': 'redshift', 'driver': 'psycopg2', 'port': 5439}
		for key in defaults:
			if key not in args:
				args[key] = defaults[key]
		
		if 'index' not in args:
			raise "Need an 'index' argument in init!"
		else:
			self.index = [x.strip() for x in args['index'].strip().split(',') if x!='']

		db_config = conf['config'][args['dbConfigName']]
		args['host'] = db_config['db_host']
		args['db'] = db_config['db_name']
		args['user'] = db_config.get('db_username', conf['db_username'])
		args['pw'] = db_config.get('db_password', conf['db_password'])

		self.name = args['name']	
		self.sql = args['sql'].strip().format(**args['format'])
		if re.search('^\w*\\.?\w+$', self.sql.strip()):
			self.sql = "select * from {table} limit 10;".format(table=self.sql.strip())
		#try to remove dangerous keywords like: delete truncate insert drop
		
		for dw in dangerous_words:
			self.sql = re.sub(dw, '', self.sql, flags=re.IGNORECASE)

		print self.sql

		conn = "{connector}+{driver}://{user}:{pw}@{host}:{port}/{db}"
		self.engine = create_engine(conn.format(**args))

	def run(self):
		df = pd.read_sql_query(self.sql, self.engine)
		if len(self.index)>0:
			df = df.set_index(self.index)
		if self.name!='':
			df.columns = [self.name+'_'+c for c in df.columns]
		return df

	def fragHash(self):
		return str(hash(self.sql+repr(self.engine)+self.name+','.join(self.index)))

#qf is a QueryFragment object
#returns df as well as its ordering within q_list (to arrange for display order of df)
def task(qf):
    frag_df = pd.DataFrame()
    try:
    	frag_df = qf.run()
    except:
    	print sys.exc_info()
    	return "Error in query!", -1

    return frag_df, qf.order

#take a list of fragments and db/substitution arguments, check if cache contains it, if not calculates and return df
def dfFromFrags(frag_list, args, cache, split_flag, clear_cache=False, join='outer'):
	df_list = [None]*len(frag_list)
	q_list = []
	hashSeedString = ''
	for i, f in enumerate(frag_list):
		print f, type(f)
		f.update(args)
		q = QueryFragment(**f)
		q.order = i
		hashSeedString += q.fragHash()
		q_list.append(q)

	queryHash = str(hash(hashSeedString+str(split_flag)+str(join)))
	if clear_cache:
		cache.delete(queryHash)
		
	df = cache.get(queryHash)
	if df is None:
		try:
			with futures.ThreadPoolExecutor(8) as executor:
				fs = [executor.submit(task, q) for q in q_list]
				for i, f in enumerate(futures.as_completed(fs)):
					if type(f.result()[0])==pd.DataFrame:
						df_list[f.result()[1]] = f.result()[0]
						print f.result()[0].index.name
					else:
						raise Exception(f.result())
			if not split_flag:
				if join!='union':
					df = pd.concat(df_list, axis=1, join=join)
				else:
					df = pd.concat(df_list)
				print type(df.index)
			else:
				df = df_list
			cache.set(queryHash, df, timeout=3600)
			print "computed df and added to cache"
		except:
			print sys.exc_info()
			return pd.DataFrame(), False, None
	else:
		print "found df in cache!"	

	return df, True, queryHash

@main.route('/')
def index():
	sql = '''select distinct table_schema, table_name, column_name from information_schema.columns
			 where table_schema!='pg_catalog';'''

	columns_df = pd.read_sql_query(sql, engine)
	dropdown_dic = retro_dictify(columns_df)

	conn_keys = json.dumps(conf['config'].keys())

	return render_template('index.html', dic=dropdown_dic, conn_options=conn_keys)

@main.route('/s3manager', methods=['GET', 'POST'])
def s3manager():
	if current_app.config.get('ES_LOCALHOST'):
		es = Elasticsearch()
	else:
		es = Elasticsearch('52.74.59.31:9200')

	if request.method == 'POST':
		r = request.form
		load_config = json.loads(r['data'])
		errors = []
		for k, v in load_config.items():
			if v!='owner' and (v is None or v==''):
				errors.append("{0} cannot be empty!".format(k))
			else:
				v = replace_dangerous(dangerous_words, v).replace(';', '')

		#Required fields are empty, pass fail back to user
		if len(errors)>0:
			return json.dumps([-1, '<br>'.join(errors)])
		else:
			cfg = {'aws_key':'awskey',
		    	   'aws_secret':'awssecret',
		    	   'where': {'event': load_config['event']}
		    	   }
			obj = s3RedShiftObj(**cfg)
			
			#see if table exists
			try:
				sql = "select * from {0} limit 10;".format(load_config['dest'])
				df = pd.read_sql_query(sql, obj.engine)
			except:
				return json.dumps([-1, repr(sys.exc_info()[1])])
		
			errors = []
			for d in pd.date_range(load_config['sd'], load_config['ed']):
				tmp_ret = obj.copy_to_redshift(d, load_config['dest'])
				if tmp_ret[0]<0:
					errors.append(tmp_ret[1])

			if load_config['owner']!='':
				change_owner_sql = "alter table {table} owner to {owner}".format(table=load_config['dest'], owner=load_config['owner'])
				with obj.engine.begin() as conn:
					conn.execute(text(change_owner_sql).execution_options(autocommit=True))

			if len(errors)==0:
				errors.append('Copy for all dates successful!')

			return json.dumps([0,'<br>'.join(errors).replace('\n', '<br>')])

	events = '''CollectableDecoration,Calendar,Achievement,claim_daily_story,weeklystory,ConnectFacebook,FishingPurchase,treenurse_collect,sterileBox_transaction,ItemUpgrade,FishingRecord,treenurse_level_board,garden_book_3star,rc_transaction,payment,SeafoodhouseProcessRecord,ItemDrop,cook_eat,cook_add,VisitNeighbors,garden_book_star,BatchProduction,Fishingbook,coins_transaction,weekly_story_start,NewOrder,barnVieayOnw,FishingStart,BeautyshopProcessRecord,start_time_machine,treenurse_kettle_trade,garden_level,Animal,LevelReward,order_refresh_all,OrderReward,BeautyshopProcessReward,luckypackage,sys_msg,MysteriousBox,activity_schedule,ever_quest,NewOrderRefresh,Kitchen,Getfreerc,buddy_gift,OnlinePackage,wheel_token_spent,fbc_vote_action,video_ballon,barnView,SeafoodhouseProcessReward,Session_end,fishing_trade,fbc_vote_collect,newuser,pre_pay,WaterWell,item_transaction,BatchProductionUpgrade,Mystery_Store_Trade,buddy,login,ItemPurchased,QuestComplete,wheel2_buy,Collect_Machine_Trade,FacebookUserInfo,welcomebackReward,UseGasoline,fbc_apply,activity_action,UseOP,stove_transaction,SellProduct,UseFarmAids,fbc_submit,lab,DailyBonus,rare_seeds,WareHouse'''.split(',')

	fn = dirname(dirname(__file__))+'/configs/clean_config.yaml'
	with open(fn, 'r') as f:
		clean_raw = list(ordered_load(f, yaml.SafeLoader))[0]['events']
	clean_configs = [{'event': k, 'days': v} for (k,v) in clean_raw.items()]

	fn = dirname(dirname(__file__))+'/configs/sql_to_es_config.yaml'
	with open(fn, 'r') as f:
		sql2es_configs = list(ordered_load(f, yaml.SafeLoader))[0]['ffs']

	max_date_dict = es_event_max_date(es)

	for s in sql2es_configs:
		empty_dict = {'max_date': 'Never', 'size': 0}
		s.update(max_date_dict.get(s['event'], empty_dict))

	events_dict = [{'value': x} for x in events]
	return render_template('s3manager.html', events=json.dumps(events_dict), clean_configs = json.dumps(clean_configs), sql2es_configs = json.dumps(sql2es_configs).encode('string_escape'))


#method for updating clean config file
@main.route('/save_clean', methods=['POST'])
def save_clean():
	if request.method == 'POST':
		r_list = json.loads(request.form['data'])
		raw_events_name = [replace_dangerous(dangerous_words, r['event'].strip().lower())  for r in r_list]
		sql = """select distinct event, lower(event) as lowered from public.events_raw where lower(event) in ('{0}');""".format("','".join(raw_events_name))
		print sql
		cfg = {'aws_key':'awskey',
		    	   'aws_secret':'awssecret',
		    	   'where': {'event': 'QuestComplete'}
		    	   }
		obj = s3RedShiftObj(**cfg)
		df = pd.read_sql_query(sql, obj.engine)
		ret_dict = {'events': {}}
		for r in r_list:
			if RepresentsInt(r['days']):
				tmp = df[df['lowered']==r['event'].strip().lower()]
				if len(tmp)>0:
					new_name = tmp['event'].values[0]
					ret_dict['events'][new_name] = int(r['days'])

		fn = dirname(dirname(__file__))+'/configs/clean_config.yaml'
		overwriteConfig(ret_dict, fn)
		return 'Done!'

@main.route('/save_sql2es', methods=['POST'])
def save_sql2es():
	if request.method == 'POST':
		r_list = json.loads(request.form['data'])
		ret_dict = {'ffs': r_list}
		fn = dirname(dirname(__file__))+'/configs/sql_to_es_config.yaml'
		overwriteConfig(ret_dict, fn)
		return 'Done!'

@main.route('/viewchart/', methods=['GET', 'POST'])
@main.route('/viewchart/<chartid>', methods=['GET', 'POST'])
def viewchart(chartid=None):
	chartid = chartid or 'default'

	if current_app.config.get('ES_LOCALHOST'):
		es = Elasticsearch()
	else:
		es = Elasticsearch('52.74.59.31:9200')
	
	fn = dirname(dirname(__file__))+'/configs/viz_config.yaml'

	if request.method == 'POST':
		r = request.form
		#print 'New request: ', r
		try:
			cfg = list(ordered_load(r['new'], yaml.SafeLoader))[0]
			print cfg
			chart = chart_info_from_config(cfg[cfg.keys()[0]], es)
			if r.get('action', '')=='save':
				overwriteConfig(cfg, fn)
			return json.dumps([0, chart])
		except:
			print sys.exc_info()
			return json.dumps([-1, repr(sys.exc_info()[1])])
	else:
		with open(fn, 'r') as f:
			configs = list(ordered_load(f, yaml.SafeLoader))[0]
		if chartid in configs:
			cfg = configs[chartid]
		else:
			cfg = configs.get('default', None)

	if cfg is None:
		raise "Default config not in config file! Please contact admin to fix."

	cfg_str = html.escape(ordered_dump(cfg, Dumper=yaml.SafeDumper, default_flow_style=False)).encode('string_escape')
	chart = chart_info_from_config(cfg, es)
	return render_template('viewchart.html', cfg=cfg_str, chart=json.dumps(chart), chartid=chartid)

@main.route('/chronicle', methods=['GET', 'POST'])
#@login_required
def chronicle():
	form = RecordForm(request.form)
	if current_app.config.get('ES_LOCALHOST'):
		es = Elasticsearch()
	else:
		es = Elasticsearch('52.74.59.31:9200')

	dropdown_opts = {}
	for c in ['app', 'event']:
		dropdown_opts[c] = get_unique(es, c)
	dropdown_opts['country'] = countries
	dropdown_opts['os'] = ['android', 'iOS']
	dropdown_opts['event'] = [{'name': x} for x in dropdown_opts['event']]
	anno_id_var = 'anno_id'

	all_annos = map(lambda x: merge_two_dicts(x['_source'], {anno_id_var: x['_id']}), es.search(index='ffskpi', doc_type='annotation', body={
                "sort": [
            {
              "date": {
                "order": "desc"
              }
            }
          ],
          "query": {
	          "bool": {
	          	"must_not": {
	          		"wildcard": {"event": "rawevents_*"}
	          	}
	          }
	        },
            "size":1000})['hits']['hits'])

	print 'all_annos:'
	print all_annos

	if request.method == 'POST' and request.form:
		print 'here we have a post:'
		print request.form
		action = request.form.get('action', None)
		if action =='export':
			return Response(pd.DataFrame(all_annos).to_csv(encoding='utf-8', index=False), mimetype='text/csv', headers={"Content-Disposition":"attachment;filename=anno_export.csv"})

		if action =='delete_anno':
			anno_id = request.form['data']
			try:
				es.delete(index='ffskpi', doc_type='annotation', id=anno_id)
				return 'Deleted anno id '+ anno_id
			except:
				print sys.exc_info()[1]
				return 'Error deleting anno id '+ anno_id

	#if user submitted annotation event
	if request.method == 'POST' and form.validate():
		tmp = {}
		
		for field in ['app', 'os', 'country_code']:
			form_field = request.form.getlist(field)
			if len(form_field)>0:
				tmp[field] = form_field
				print field, form_field
		
		if 'country_code'in tmp:
			tmp['country'] = [country_dict[x] for x in tmp['country_code']]
		
		event = request.form.getlist('event')
		if len(event)==0:
			flash('Need to specify an event!')
			return redirect(url_for('main.chronicle'))
		else:
			tmp['event'] = event

		tmp['date'] = form.sd.data
		tmp['duration'] = max(form.dur.data, 1)
		tmp['comment'] = form.comment.data
		if form.url.data!='':
			tmp['link'] = form.url.data

		hash_str = ''.join([str(tmp.get(x, '')) for x in ['date', 'app', 'event', 'os', 'country']])
		tmp_id = hash(hash_str)
		print 'hashes:', hash_str, tmp_id
		ret = es.index(index="ffskpi", doc_type='annotation', id=tmp_id, body=tmp)
		flash('Record added to database')
		return redirect(url_for('main.chronicle'))

	with open(dirname(dirname(__file__))+'/configs/viz_config.yaml', 'r') as f:
		configs = list(ordered_load(f, yaml.SafeLoader))[0]
	"""
	Each config section looks like this:
	  ffs:
	    lines:
		- event: dau
		  name: dau
		  splits: app
		  agg: sum(v2)
		  freq: day
		  from: now-120d/d
		  to: now
		- event: dau
		  name: arpu
		  freq: day
		  agg: sum(v1)/sum(v2)
		  y2: true
		  from: now-120d/d
		annos:
		  event: dau
		grid:
		  x: 0
		  y: 6
		  width: 8
		  height: 2"""	
	
	charts = []
	for config_key, chart_config in configs.items():
		grid_config = chart_config['grid']
		tab_id = int(chart_config.get('tab', 0))
		chart_obj = chart_info_from_config(chart_config, es)
		chart_obj['grid'] = grid_config
		chart_obj['chartid'] = config_key
		chart_obj['tab'] = tab_id
		charts.append(chart_obj)

	if len(all_annos)>0:
		annos_df = pd.DataFrame(all_annos).drop(anno_id_var, axis=1)
		annos_df['action'] = "<p><a class='editAnno' href='#'>Edit</a>&nbsp;<a class='delAnno' href='#'>Delete</a></p>"
		annos_df = annos_df.set_index('date').to_html(classes=["table", "table-striped", "table-sm", "table-condensed"], escape=False)
	else: 
		annos_df = pd.DataFrame().to_html()

	return render_template('chronicle.html', objs=json.dumps(charts), form=form, dropdown_opts=json.dumps(dropdown_opts).encode('string_escape'), latest_annos = annos_df, anno_objs = json.dumps(all_annos))

#generate trend table and xy for charting purposes for a df
#trend_intervals indicate the intervals to calculate rolling mean for
def trend_xy(df, trend_intervals=[1,3,10]):
	response = {}
	response['errors'] = ''

	try:
		chart_xy = {}
		for c in df.columns:
		    tmp = df[c].reset_index()
		    tmp.columns = ['x', 'y']
		    chart_xy[c] = json.loads(tmp.to_json(orient='records'))
		response['chart_xy'] = json.dumps(chart_xy)
	except:
		print sys.exc_info()
		print "Failed to calculate chart xy."
		response['errors']+=str(sys.exc_info())

	#try generating trend df
	try:
		rm_df = df_rolling_mean(df, intervals=trend_intervals, include_self=False, prefix='mean_', agg_func=sum)
		print df.head().to_string()
		print df.index.name
		df_diff = rm_df.sort_values(by=[df.index.name, 'key']).iloc[-len(df.columns):].set_index(df.index.name)
		key_col_index = -1
		for i in range(1, len(df_diff.columns)):
			if df_diff.columns[i]!='key':
				df_diff.iloc[:, i] = df_diff.iloc[:, 0] - df_diff.iloc[:, i]
			else:
				key_col_index = i
		df_diff['key_label'] = df_diff['key']

		x = ET.fromstring(df_diff.to_html(classes=["rolling"]))

		for z in x.findall('.//tbody/tr'):
		    for i, c in enumerate(z.getchildren()):
		    	if i>=1:
			        if i==1:
			            orig = float(c.text)
			            c.attrib={}
			        #need the +1 for key_col_index for alignment...
			        elif i<key_col_index+1:
			            delta = float(c.text)
			            val = orig-delta
			            if val==0:
			            	val=0.1
			            pct = delta*100/val
			            space = ' ('
			            if pct>0:
			                space+='+'
			            c.text = '{:.2f}'.format(delta)+space+'{:.1f}%'.format(pct)+')'
			            if delta>0:
			                c.set('style', 'color: green')
			            else:
			                c.set('style', 'color: red')
			        elif i==key_col_index+1:
			        	c.set('data-key', c.text)
		
		response['trend_df'] = ET.tostring(x)
		response['is_timeseries'] = True
	except:
		print sys.exc_info()
		print "Failed to calculate trends."
		response['errors']+=str(sys.exc_info())

	return response


@main.route('/compare', methods=['GET', 'POST'])
#@login_required
def compare():
	if request.method == 'POST':
		r = request.form
		configs = json.loads(r['data'])
		action = configs['action']
		frag_configs = configs['frags']
		param_configs = configs['params']
		pivot_configs = configs.get('pivot', {})
		join_flag = configs['joinFlag']

		#legacy reasons...keeping split_flag so that i dont have to change implemention of dfFromFrags
		split_flag = False
		if join_flag == 'split':
			split_flag = True
		
		#build dict to pass from param_comfigs
		params_dict = {}
		for param in param_configs:
			if param['param']!='':
				params_dict[param['param']] = param['selected']

		print param_configs
		print params_dict
		print pivot_configs
		print split_flag

		test_flag = False
		if test_flag:
			#df_raw = [pd.DataFrame(np.random.rand(14,3), columns='alpha beta gamma'.split(), index=list(lowercase[:14])), pd.DataFrame(np.random.rand(14,3), columns='alpha beta gamma'.split(), index=list(lowercase[:14]))]
			df_raw = [pd.DataFrame(np.random.rand(14,3), columns='alpha beta gamma'.split(), index=list(lowercase[:14])), pd.DataFrame(np.random.rand(14,3), columns='alpha beta gamma'.split(), index=pd.Series(pd.date_range(date(2016,1,1), periods=14), name='date'))]
			
			success_flag = True
			queryHash = 'abc'
		else:
			clear_cache = False
			if action=='refresh':
				clear_cache = True

			df_raw, success_flag, queryHash = dfFromFrags(frag_configs, {'format': params_dict}, cache, split_flag, clear_cache, join_flag)
		
		if split_flag:
			df_list = df_raw[:]
		else:
			df_list = [df_raw]

		response_list = []
		total_response = {}

		if success_flag:
			for df_original in df_list:
				response = {}
				if len(pivot_configs.get('index', []))*len(pivot_configs.get('columns',[]))*len(pivot_configs.get('value','').strip())>0:
					try:
						df= df_original.reset_index().groupby(pivot_configs['index']+pivot_configs['columns'])[pivot_configs['value'].strip()].sum().unstack(pivot_configs['columns']).fillna(0)
						#flatten columns if multiindex
						if type(df.columns)==pd.MultiIndex:
							df.columns = ["/".join([str(x).strip() for x in col]) for col in df.columns.values]
					except:
						print sys.exc_info()
						print "Pivoting failed!"
						df = df_original
				else:
					df = df_original

				#if the values of an entire column is between 0 and 1, display it as a %
				numerics = ['float16', 'float32', 'float64']
				
				for c in df.select_dtypes(include=numerics).columns:
					if df[c].max()<=1 and df[c].min()>=0:
						df[c] = df[c].map(lambda x: '{:.1f}%'.format(x*100) if ~np.isnan(x) else '')

				response['df'] = df.iloc[-100:].to_html(classes=["table", "table-striped", "table-sm", "table-condensed"])
				response['hash'] = queryHash
				response.update(trend_xy(df))
				response_list.append(response)

			if action=='export':
				return Response('\n'.join([df_original.to_csv() for df_original in df_list]), mimetype='text/csv', headers={"Content-Disposition":"attachment;filename=export.csv"})

			total_response['params'] = param_configs
			total_response['dfs'] = response_list

		#something went wrong with the query
		else:
			total_response['error'] = "Your query failed for some reason!"

		return json.dumps(total_response)

	#if visiting the page directly
	return render_template('compare.html', df=pd.DataFrame().to_html(), db_configs=json.dumps(db_configs), session=session)

#We assume that the key of cfg(dict) represents the unique id of the config object in file, and that 
#we have already validated cfg
def overwriteConfig(cfg, filename):
	with open(filename, 'r') as f:
		old_configs = list(ordered_load(f, yaml.SafeLoader))[0]
	if cfg.keys()[0] in old_configs:
		old_configs.pop(cfg.keys()[0])
	old_configs.update(cfg)
	with open(filename, 'w') as outfile:
		outfile.write(ordered_dump(old_configs, Dumper=yaml.SafeDumper, default_flow_style=False))

#update deleted/latest grid info in config file
@main.route('/save_grid', methods=['POST'])
def saveGrid():
	r = request.form
	new_params = json.loads(r['data'])
	print new_params
	with open(dirname(dirname(__file__))+'/configs/viz_config.yaml', 'r') as f:
		configs = list(ordered_load(f, yaml.SafeLoader))[0]

	try:
		for key in configs:
			if key not in new_params:
				configs.pop(key)
			else:
				configs[key]['grid'] = new_params[key]
		with open(dirname(dirname(__file__))+'/configs/viz_config.yaml', 'w') as outfile:
			outfile.write(ordered_dump(configs, Dumper=yaml.SafeDumper, default_flow_style=False))
		return json.dumps(True)
	except:
		print sys.exc_info()[1]
		return json.dumps(False)

@main.route('/save_query', methods=['POST'])
def saveQuery():
	r = request.form
	configs = json.loads(r['data'])
	fn = dirname(dirname(__file__))+'/configs/query_config.yaml'
	overwriteConfig(configs, fn)
	return "Success!"

@main.route('/load_queries', methods=['POST'])
def loadQueries():
	with open(dirname(dirname(__file__))+'/configs/query_config.yaml', 'r') as f:
		configs = list(ordered_load(f, yaml.SafeLoader))[0]
	print configs
	return json.dumps(configs)

@main.route('/dashboard')
def dashboard():
	sql = '''select date_trunc('{freq}', date) as date, app,
            count(distinct snsid) as au,
            sum(is_new_user) as new,
            sum(revenue_usd) as rev
            from processed.fact_dau_snapshot
            --where datediff('month', date, sysdate)<=12
            group by 1,2
            order by 1,2;'''
	#df = pd.read_sql_query(sql.format(freq='week'), engine)
	#d2 = set_datetimeindex(df, 'date')
	#d2_comp = df_rolling_mean(d2, intervals=[1,3,10])
	#ret = d2_comp.sort_values(by=['date', 'key']).dropna(0).tail(6).to_html(classes='index')

	#test code to generate colored table on server
	df = pd.DataFrame(np.random.rand(14,3))
	df.columns = list('abc')
	df.index = pd.date_range(date(2016,1,1), periods=len(df))
	d2 = df_rolling_mean(df, intervals=[1,3,10], include_self=False, prefix='mean_', agg_func=sum)

	sparkline_dict = {}
	for c in df.columns:
	    tmp = df[c].reset_index()
	    tmp.columns = ['x', 'y']
	    sparkline_dict[c] = json.loads(tmp.to_json(orient='records'))

	print json.dumps(sparkline_dict)

	df_diff = d2.sort_values(by=['index', 'key']).iloc[-3:].set_index('index')
	key_col_index = -1
	for i in range(1, len(df_diff.columns)):
		if df_diff.columns[i]!='key':
			df_diff.iloc[:, i] = df_diff.iloc[:, 0] - df_diff.iloc[:, i]
		else:
			key_col_index = i


	print key_col_index
	print df_diff.to_string()
	x = ET.fromstring(df_diff.to_html(classes='rolling'))

	for z in x.findall('.//tbody/tr'):
	    for i, c in enumerate(z.getchildren()):
	    	if i>=1:
		        if i==1:
		            orig = float(c.text)
		            c.attrib={}
		        #need the +1 for key_col_index for alignment...
		        elif i!=key_col_index+1:
		            delta = float(c.text)
		            val = orig-delta
		            pct = delta*100/val
		            space = ' ('
		            if pct>0:
		                space+='+'
		            c.text = '{:.2f}'.format(delta)+space+'{:.1f}%'.format(pct)+')'
		            if delta>0:
		                c.set('style', 'color: green')
		            else:
		                c.set('style', 'color: red')
		        else:
		        	c.set('data-key', c.text)

	return render_template('dashboard.html', df=ET.tostring(x), sparklines=json.dumps(sparkline_dict))

@main.route('/get_schema', methods=['POST'])
def get_schema():
	if request.form:
		r = request.form
		d = json.loads(r.keys()[0])
		
		sql = '''SELECT * FROM {schema}.{table}
			--order by md5('seed' || ts || user_id)
			LIMIT 100;'''.format(schema=d[0], table=d[1])
		df = pd.read_sql_query(sql, engine)

		schema_obj = {}
		schema_obj['columns'] = df.columns.tolist()

		if 'properties' in df.columns:
			keys = set()
			for v in df['properties'].values:
				j = json.loads(v)
				for k in j:
					keys.add(k)

			#query = 'select \n'
			#query += ',\n'.join(['json_extract_path_text(properties, \''+ k+'\') as '+ k for k in keys])
			#query += '\nfrom dev.raw_events.session_start'

			schema_obj['properties'] = list(keys)

		return json.dumps(schema_obj)

@main.route('/pivot', methods=['GET', 'POST'])
def pivot():
	
	def compose2(f, g):
		return lambda x: f(g(x))

	count_distinct = compose2(func.count, distinct)

	filters = {}
	pivots = {}
	agg_param_dict = {}
	agg_param_names = {}
	agg_funcs_map = {'sum': func.sum, 'max': func.max, 'count': func.count, 'min': func.min, 'count distinct': count_distinct}

	#if we are getting form data
	if request.method == 'POST':
		r = request.form
		print r
		for k in r:
			l = r.getlist(k)
			if len(l)>0:
				if k in ['pivots']:
					pivots[k] = l
				elif not k.startswith('aggfunc_'):
					filters[k] = l
				else:
					if l[0]!='None':
						k1 = k.replace('aggfunc_','')
						agg_param_dict[k1] = agg_funcs_map[l[0]]
						agg_param_names[k1] = l[0]


	metadata = MetaData()
	table_name = 'agg_kpi'
	#table_name = 'agg_kpi'
	messages = Table(table_name, metadata, Column("dummy", Integer, primary_key=True), schema='processed', autoload=True, autoload_with=engine)

	aggable_cols = []
	for c in messages.c:
		if c.name!='dummy':
			if c.type.__str__() in ['SMALLINT', 'INTEGER', 'BIGINT'] or c.type.__str__().startswith('NUMERIC'):
				aggable_cols.append(c.name)

	Base = automap_base(metadata=metadata)
	Base.prepare()
	b = getattr(Base.classes, table_name)

	Session = sessionmaker(bind=engine)
	session = Session()

	colnames = [x.name for x in b.__table__.columns if x.name!='dummy']
	u = [func.count(distinct(getattr(b, x))) for x in colnames]
	get_uniques = session.query(*u)

	threshold = 20
	#gu = get_uniques.all()[0]
	filter_keys = []
	'''for i in range(len(gu)):
		if gu[i]<=threshold:
			filter_keys.append(colnames[i])'''

	filter_options = {}
	selected_options = {}
	for d in filter_keys:
		temp = {}
		temp['field'] = d
		temp['values'] = [str(x[0]) for x in session.query(distinct(getattr(b, d))).all()]
		filter_options.append(temp)
		selected_options[d] = []

	for k in ['pivots']:
		selected_options[k] = []
		for c in colnames:
			tmp = {}
			tmp['value'] = c
			tmp['checked'] = (c in pivots.get(k, []))
			selected_options[k].append(tmp)


	#filter_dict contains all the info we want to send to frontend
	filter_dict = {}
	filter_dict['filters'] = filter_options
	filter_dict['selected'] = selected_options
	filter_dict['columns'] = colnames

	filter_dict['selected']['agg'] = agg_param_names

	pivots = pivots.get('pivots', [])
	#pivot_cols = [x for x in pivots.get('pivot_col', []) if x not in pivot_rows]

	
	
	agg_funcs_options = ['None'] + agg_funcs_map.keys()
	#agg_param_dict = {'count': func.sum, 'user_count': count_distinct}

	aggs = {}
	aggs['funcs'] = agg_funcs_options
	aggs['aggcols'] = aggable_cols

	#f = func.max
	
	groupby_names = [i for i in pivots if i not in agg_param_dict.keys()]
	groupbys = [getattr(b, i) for i in groupby_names]
	ac = [agg_param_dict[k](getattr(b, k)) for k in agg_param_dict]+[func.count('*')]
	expanded = groupbys + ac
	colnames =  groupby_names + agg_param_dict.keys()

	our_user = session.query(*expanded).group_by(*groupbys)

	if filters:
		for k in filters:
			if type(filters[k])==list:
				our_user = our_user.filter(getattr(b, k).in_(filters[k]))

	print str(our_user)

	df = pd.DataFrame(our_user.all())
	df.columns = colnames+['table_count']

	#df.columns = [i['expr'].name for i in our_user.column_descriptions]
	raw_df = df.copy()

	'''if len(groupbys)>0:
		df = df.set_index(pivot_rows+pivot_cols)
	if len(pivot_cols)>0:
		df = df.unstack(pivot_cols).fillna(0)
	if len(pivot_rows)==0:
		df = pd.DataFrame(df.iloc[:5]).T'''

	ret = df.head().to_html()
	data_json = raw_df.to_json(orient='records', date_format='iso').replace('\'', '\\u0027')

	return render_template('visualize.html', df = ret, filters = json.dumps(filter_dict).replace('\'','\\u0027'), data_json=data_json, aggs=json.dumps(aggs))

@main.route('/loadfilter', methods=['POST'])
def loadfilter():
	r = request.form
	print r
	
	metadata = MetaData()
	schema = r['schema']
	table_name = r['table']
	field = r['field']
	messages = Table(table_name, metadata, Column("dummy", Integer, primary_key=True), schema=schema, autoload=True, autoload_with=engine)

	Base = automap_base(metadata=metadata)
	Base.prepare()
	b = getattr(Base.classes, table_name)
	Session = sessionmaker(bind=engine)
	session = Session()
	dist_values = [str(x[0]) for x in session.query(distinct(getattr(b, field))).all()]
	
	return json.dumps(dist_values)