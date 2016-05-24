import pandas as pd
from sqlalchemy import create_engine, text
import sqlalchemy
import numpy as np
import yaml
from collections import OrderedDict
from pyparsing import Word, oneOf, Literal, nums, ZeroOrMore, Optional,Group, Combine, Forward
import sys
import pprint
import elasticsearch.helpers as eshelpers
from elasticsearch.helpers import bulk, streaming_bulk
import re

#takes a pandas series, a list of rolling mean intervals, and return a df that puts these rolling means together
#args:
# intervals - list of integers that define lengths of rolling windows
# include_self - if false (default), shift up 1 row before calculations
# output_pct - if true, output % of rolling means over original
# prefix - column prefixes of output, like 'mean_7'
def RepresentsInt(s):
    try: 
        int(s)
        return True
    except ValueError:
        return False

def replace_dangerous(danger_word_list, word):
    for dw in danger_word_list:
        word = re.sub(dw, '', word, flags=re.IGNORECASE)
    return word

country_dict = {'AD': 'Andorra',
 'AE': 'United Arab Emirates',
 'AF': 'Afghanistan',
 'AG': 'Antigua And Barbuda',
 'AI': 'Anguilla',
 'AL': 'Albania',
 'AM': 'Armenia',
 'AN': 'Netherlands Antilles',
 'AO': 'Angola',
 'AQ': 'Antarctica',
 'AR': 'Argentina',
 'AS': 'American Samoa',
 'AT': 'Austria',
 'AU': 'Australia',
 'AW': 'Aruba',
 'AX': 'Aland Islands',
 'AZ': 'Azerbaijan',
 'BA': 'Bosnia And Herzegovina',
 'BB': 'Barbados',
 'BD': 'Bangladesh',
 'BE': 'Belgium',
 'BF': 'Burkina Faso',
 'BG': 'Bulgaria',
 'BH': 'Bahrain',
 'BI': 'Burundi',
 'BJ': 'Benin',
 'BL': 'Saint Barthelemy',
 'BM': 'Bermuda',
 'BN': 'Brunei Darussalam',
 'BO': 'Bolivia, Plurinational State Of',
 'BR': 'Brazil',
 'BS': 'Bahamas',
 'BT': 'Bhutan',
 'BV': 'Bouvet Island',
 'BW': 'Botswana',
 'BY': 'Belarus',
 'BZ': 'Belize',
 'CA': 'Canada',
 'CC': 'Cocos (Keeling) Islands',
 'CD': 'Congo, The Democratic Republic Of The',
 'CF': 'Central African Republic',
 'CG': 'Congo',
 'CH': 'Switzerland',
 'CI': "Cote D'Ivoire",
 'CK': 'Cook Islands',
 'CL': 'Chile',
 'CM': 'Cameroon',
 'CN': 'China',
 'CO': 'Colombia',
 'CR': 'Costa Rica',
 'CU': 'Cuba',
 'CV': 'Cape Verde',
 'CX': 'Christmas Island',
 'CY': 'Cyprus',
 'CZ': 'Czech Republic',
 'DE': 'Germany',
 'DJ': 'Djibouti',
 'DK': 'Denmark',
 'DM': 'Dominica',
 'DO': 'Dominican Republic',
 'DZ': 'Algeria',
 'EC': 'Ecuador',
 'EE': 'Estonia',
 'EG': 'Egypt',
 'EH': 'Western Sahara',
 'ER': 'Eritrea',
 'ES': 'Spain',
 'ET': 'Ethiopia',
 'FI': 'Finland',
 'FJ': 'Fiji',
 'FK': 'Falkland Islands (Malvinas)',
 'FM': 'Micronesia, Federated States Of',
 'FO': 'Faroe Islands',
 'FR': 'France',
 'GA': 'Gabon',
 'GB': 'United Kingdom',
 'GD': 'Grenada',
 'GE': 'Georgia',
 'GF': 'French Guiana',
 'GG': 'Guernsey',
 'GH': 'Ghana',
 'GI': 'Gibraltar',
 'GL': 'Greenland',
 'GM': 'Gambia',
 'GN': 'Guinea',
 'GP': 'Guadeloupe',
 'GQ': 'Equatorial Guinea',
 'GR': 'Greece',
 'GS': 'South Georgia And The South Sandwich Islands',
 'GT': 'Guatemala',
 'GU': 'Guam',
 'GW': 'Guinea-Bissau',
 'GY': 'Guyana',
 'HK': 'Hong Kong',
 'HM': 'Heard Island And Mcdonald Islands',
 'HN': 'Honduras',
 'HR': 'Croatia',
 'HT': 'Haiti',
 'HU': 'Hungary',
 'ID': 'Indonesia',
 'IE': 'Ireland',
 'IL': 'Israel',
 'IM': 'Isle Of Man',
 'IN': 'India',
 'IO': 'British Indian Ocean Territory',
 'IQ': 'Iraq',
 'IR': 'Iran, Islamic Republic Of',
 'IS': 'Iceland',
 'IT': 'Italy',
 'JE': 'Jersey',
 'JM': 'Jamaica',
 'JO': 'Jordan',
 'JP': 'Japan',
 'KE': 'Kenya',
 'KG': 'Kyrgyzstan',
 'KH': 'Cambodia',
 'KI': 'Kiribati',
 'KM': 'Comoros',
 'KN': 'Saint Kitts And Nevis',
 'KP': "Korea, Democratic People'S Republic Of",
 'KR': 'Korea, Republic Of',
 'KW': 'Kuwait',
 'KY': 'Cayman Islands',
 'KZ': 'Kazakhstan',
 'LA': "Lao People'S Democratic Republic",
 'LB': 'Lebanon',
 'LC': 'Saint Lucia',
 'LI': 'Liechtenstein',
 'LK': 'Sri Lanka',
 'LR': 'Liberia',
 'LS': 'Lesotho',
 'LT': 'Lithuania',
 'LU': 'Luxembourg',
 'LV': 'Latvia',
 'LY': 'Libyan Arab Jamahiriya',
 'MA': 'Morocco',
 'MC': 'Monaco',
 'MD': 'Moldova, Republic Of',
 'ME': 'Montenegro',
 'MF': 'Saint Martin',
 'MG': 'Madagascar',
 'MH': 'Marshall Islands',
 'MK': 'Macedonia, The Former Yugoslav Republic Of',
 'ML': 'Mali',
 'MM': 'Myanmar',
 'MN': 'Mongolia',
 'MO': 'Macao',
 'MP': 'Northern Mariana Islands',
 'MQ': 'Martinique',
 'MR': 'Mauritania',
 'MS': 'Montserrat',
 'MT': 'Malta',
 'MU': 'Mauritius',
 'MV': 'Maldives',
 'MW': 'Malawi',
 'MX': 'Mexico',
 'MY': 'Malaysia',
 'MZ': 'Mozambique',
 'NA': 'Namibia',
 'NC': 'New Caledonia',
 'NE': 'Niger',
 'NF': 'Norfolk Island',
 'NG': 'Nigeria',
 'NI': 'Nicaragua',
 'NL': 'Netherlands',
 'NO': 'Norway',
 'NP': 'Nepal',
 'NR': 'Nauru',
 'NU': 'Niue',
 'NZ': 'New Zealand',
 'OM': 'Oman',
 'PA': 'Panama',
 'PE': 'Peru',
 'PF': 'French Polynesia',
 'PG': 'Papua New Guinea',
 'PH': 'Philippines',
 'PK': 'Pakistan',
 'PL': 'Poland',
 'PM': 'Saint Pierre And Miquelon',
 'PN': 'Pitcairn',
 'PR': 'Puerto Rico',
 'PS': 'Palestinian Territory, Occupied',
 'PT': 'Portugal',
 'PW': 'Palau',
 'PY': 'Paraguay',
 'QA': 'Qatar',
 'RE': 'Reunion',
 'RO': 'Romania',
 'RS': 'Serbia',
 'RU': 'Russian Federation',
 'RW': 'Rwanda',
 'SA': 'Saudi Arabia',
 'SB': 'Solomon Islands',
 'SC': 'Seychelles',
 'SD': 'Sudan',
 'SE': 'Sweden',
 'SG': 'Singapore',
 'SH': 'Saint Helena, Ascension And Tristan Da Cunha',
 'SI': 'Slovenia',
 'SJ': 'Svalbard And Jan Mayen',
 'SK': 'Slovakia',
 'SL': 'Sierra Leone',
 'SM': 'San Marino',
 'SN': 'Senegal',
 'SO': 'Somalia',
 'SR': 'Suriname',
 'ST': 'Sao Tome And Principe',
 'SV': 'El Salvador',
 'SY': 'Syrian Arab Republic',
 'SZ': 'Swaziland',
 'TC': 'Turks And Caicos Islands',
 'TD': 'Chad',
 'TF': 'French Southern Territories',
 'TG': 'Togo',
 'TH': 'Thailand',
 'TJ': 'Tajikistan',
 'TK': 'Tokelau',
 'TL': 'Timor-Leste',
 'TM': 'Turkmenistan',
 'TN': 'Tunisia',
 'TO': 'Tonga',
 'TR': 'Turkey',
 'TT': 'Trinidad And Tobago',
 'TV': 'Tuvalu',
 'TW': 'Taiwan, Province Of China',
 'TZ': 'Tanzania, United Republic Of',
 'UA': 'Ukraine',
 'UG': 'Uganda',
 'UM': 'United States Minor Outlying Islands',
 'US': 'United States',
 'UY': 'Uruguay',
 'UZ': 'Uzbekistan',
 'VA': 'Holy See (Vatican City State)',
 'VC': 'Saint Vincent And The Grenadines',
 'VE': 'Venezuela, Bolivarian Republic Of',
 'VG': 'Virgin Islands, British',
 'VI': 'Virgin Islands, U.S.',
 'VN': 'Viet Nam',
 'VU': 'Vanuatu',
 'WF': 'Wallis And Futuna',
 'WS': 'Samoa',
 'YE': 'Yemen',
 'YT': 'Mayotte',
 'ZA': 'South Africa',
 'ZM': 'Zambia',
 'ZW ': 'Zimbabwe'}

countries = [{'country_code': x[0], 'country_name': x[1]} for x in sorted(country_dict.items())]
    
def rolling_multi_period(series, intervals=[1,7,30], include_self=False, func='mean', prefix='mean_'):
    if include_self:
        series_shifted = series
    else:
        series_shifted = series.shift(1)
    
    out_list = [series]
    for i in intervals:
        if type(i)==int:
            if func=='sum':
                rolling_func = pd.rolling_sum
                if prefix=='mean_':
                    prefix = 'sum_'
            else:
                rolling_func = pd.rolling_mean
            n = rolling_func(series_shifted, i, min_periods=1)
            #if output_pct:
            #    n = series.div(n)
            n.name = prefix+str(i)
            out_list.append(n)
    
    return pd.concat(out_list, axis=1)

#calculate rolling mean comparison for each column in df. index needs to be datetimeindex.
#if there are duplicate entries in the datetimeindex, we group by index and apply the custom agg_func
#before calculations.
#args:
# intervals - list of integers that define lengths of rolling windows
# include_self - if false (default), shift up 1 row before calculations
# output_pct - if true, output % of rolling means over original
# prefix - column prefixes of output, like 'mean_7'
# agg_func - function or dict of column:function (ala pandas). sum by default.

def df_rolling_mean(df, intervals=[1,7,30], include_self=False, prefix='mean_', agg_func=sum):
    try:
        df.index = pd.to_datetime(df.index)
    except:
        raise Exception('Failed to convert index to datetime index!')
    #if df.index.nunique()!=len(df):
    #    raise Exception('Duplicate values detected in datetimeindex!')
    
    ret_list = []
    num_df = df._get_numeric_data()
    if len(num_df.columns)>0:
        num_df = num_df.groupby(num_df.index).apply(agg_func)
        for c in num_df.columns:
            r = rolling_multi_period(num_df[c], intervals, include_self)
            r.columns = ['value'] + r.columns.tolist()[1:]
            r['key'] = c
            ret_list.append(r.reset_index())
        
    ret = pd.concat(ret_list)
    ret.index = range(len(ret))
    return ret

#helper function to convert a column to pandas datetime and making it the index
def set_datetimeindex(df, col):
    tmp = df.set_index(col)
    tmp.index = pd.to_datetime(tmp.index)
    return tmp

def ordered_dump(data, stream=None, Dumper=yaml.Dumper, **kwds):
    class OrderedDumper(Dumper):
        pass
    def _dict_representer(dumper, data):
        return dumper.represent_mapping(
            yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
            data.items())
    OrderedDumper.add_representer(OrderedDict, _dict_representer)
    return yaml.dump(data, stream, OrderedDumper, **kwds)

def ordered_load(stream, Loader=yaml.Loader, object_pairs_hook=OrderedDict):
    class OrderedLoader(Loader):
        pass
    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))
    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load_all(stream, OrderedLoader)
# usage:
#ordered_dump(data, Dumper=yaml.SafeDumper)
# with open('waf_schema_all.yml', 'w') as outfile:
#    outfile.write(ordered_dump(ret, Dumper=yaml.SafeDumper))

#given the arguments, returns the es query json body that would do aggregations
def es_search_body(event, sd="now-10d/d", splits=[], aggexpr="sum(v1)", ed="now", freq='day', **kwargs):
    agg_dict = {}
    sub_expr, var_dict = parse_expr(aggexpr)
    var_mappings = {'v1': 'value', 'v2': 'value2'}
    ident_dict = {}
        
    if freq:
        agg_dict['date'] = {
                            "date_histogram" : {
                                "field" : "date",
                                "interval" : freq,
                                "format" : "yyyy-MM-dd" 
                                },
                            "aggs": {}
                            }
        present_dict = agg_dict['date']['aggs']
    else:
        present_dict = agg_dict
        
    for c in splits:
        present_dict[c] = {"terms": {"field": c}, "aggs": {}}
        present_dict = present_dict[c]["aggs"]
    
    for k, v in var_dict.items():
        present_dict[v] = {k[0]: {'field': var_mappings[k[1]]}}
        ident_dict[v] = v
    present_dict['outcome'] = {'bucket_script': 
                                                   {'buckets_path': ident_dict,
                                                'script': sub_expr}
                                }
        
    skeleton = {"query": {
                    "filtered": {
                        "filter": {
                            "bool": {
                                "must": [
                                    {"range" : {
                                            "date" : {
                                                "gte" : sd,
                                                "lt" :  ed
                                            }
                                        }},
                                    {"wildcard": {
                                        "event": event 
                                    }}
                                ]
                            }
                        }
                    }
                },
            "aggs": agg_dict
            }

    skeleton_must_list = skeleton['query']['filtered']['filter']['bool']['must']
    for c in ['country', 'app']:
        if c in kwargs:
            skeleton_must_list.append({"terms": {c: kwargs[c]}})

    
    return skeleton

#agg_obj is the es obj after running search and aggregation
#can see if we can optimize implementation later
def es_flatten_aggs(agg_obj):
    key = unicode(agg_obj.keys()[0])
    obj = agg_obj[key]
    assert 'buckets' in obj
    ret = []
    bottom = True
    next_key = None
    #check if we are at the bottom of the obj
    content = obj['buckets']
    for k,v in content[0].items():
        if (type(v)==dict) and ('buckets' in v):
            bottom = False
            next_key = k
    
    if bottom:
        for c in content:
            tmp = {}
            if 'key_as_string' in c:
                tmp[key] = c['key_as_string']
            elif 'key' in c and type(c['key']) in (str, unicode):
                tmp[key] = c['key']
            for k,v in c.items():
                if type(v)==dict and ('value' in v):
                #we found our value!
                    tmp[k] = v['value']
            ret.append(tmp)
    elif next_key:
        for c in content:
            tmp = {}
            if 'key_as_string' in c:
                tmp[key] = c['key_as_string']
            elif 'key' in c and type(c['key']) in (str, unicode):
                tmp[key] = c['key']

            arr = es_flatten_aggs(c)
            for a in arr:  
                a.update(tmp)
                ret.append(a)
    
    return ret

def extract_xy_anno(df, x, y, name, groupbys=None, anno_objs=None, anno_text=None, anno_x=None):
        xys = []

        if groupbys:
            for k, grouped in df.groupby(groupbys):
                tmp = grouped.sort_values(by=x)[[x,y]].rename(columns={x: 'x', y: 'y'}).to_dict(orient='list')
                if type(k) in (list, tuple):
                    tmp['name'] = name+'_'+'/'.join(k)
                else:
                    tmp['name'] = name+'_'+k
                xys.append(tmp)
        else:
            tmp = df.sort_values(by=x)[[x,y]].rename(columns={x: 'x', y: 'y'}).to_dict(orient='list')
            tmp['name'] = name
            xys.append(tmp)

        annos = []
        if anno_objs and anno_text:
            if not anno_x:
                anno_x = x
            for o in anno_objs:
                tmp = {}
                z = df[df[x]==o[anno_x]]
                """if len(z)>0:
                    tmp['yref'] = 'y'
                    tmp['y'] = z[y].max()
                else:"""
                #tmp['yref'] = 'paper'
                tmp['y'] = 0
                tmp['text'] = ','.join(o['event'])
                tmp['x'] = o[anno_x]
                tmp['comment'] = o[anno_text]
                tmp['duration'] = o.get('duration', 1)
                if 'link' in o:
                    tmp['link'] = o['link']
                annos.append(tmp)

        return xys, annos

def merge_two_dicts(x, y):
    '''Given two dicts, merge them into a new dict as a shallow copy.'''
    z = x.copy()
    z.update(y)
    return z
    
def get_unique(es, field, index='ffskpi'):
    """get unique values of field in es"""
    body = {
    "size": 0,
    "query": {
              "bool": {
                "must_not": {
                    "wildcard": {"event": "rawevents_*"}
                }
              }
            },
    "aggs" : {
        "out" : {
            "terms" : { "field" : field, "size": 0}
        }
    }}

    r = [x['key'] for x in es.search(index=index, filter_path=['aggregations'], body=body)['aggregations']['out']['buckets']]

    return r

#use pyparsing to parse expressions like sum(v1)/sum(v2), etc
def sub_token(t, var_list):
    if tuple(t[0]) not in var_list:
        x = 'x'+str(len(var_list)+1)
        var_list[tuple(t[0])] = x
    else:
        x = var_list[tuple(t[0])]
    return [x]

def parse_expr(expr_str):
    """returns a subbed expression + variable dictionary - e.g. sum(v1)/sum(v2) becomes x1/x2, {(sum, v1): x1, (sum, v2): x2}"""
    var_list = {}
    
    aggfuncs = oneOf("sum avg min max")
    field = oneOf("v1 v2")
    ops = Word("+-*/", max=1)
    lpar  = Literal( "(" ).suppress()
    rpar  = Literal( ")" ).suppress()
    block = Group(aggfuncs+lpar+field+rpar).setParseAction(lambda x: sub_token(x, var_list))
    atom = block | Word(nums)
    calc = atom + ZeroOrMore(ops+atom) | "("+atom + ZeroOrMore(ops+atom)+")"
    expr = calc + ZeroOrMore(ops+calc)
    
    return ''.join(expr.parseString(expr_str)), var_list

def chart_info_from_config(cfg, es):
    """returns a chart obj (xys/annos) from a config dict (loaded from config file).
        cfg is the config dict for a chart
        grid config is not part of this function"""
    xys = []
    annos = []

    lines_config = cfg['lines']
    anno_event = cfg.get('annos', {}).get('event', None)

    for i, lc in enumerate(lines_config):
        if 'event' not in lc:
            pass
        event = lc['event']
        names = [x.strip() for x in lc.get('name', event+str(i)).split(',') if x!='']
        sd = str(lc.get('from', "now-10d/d"))
        ed = str(lc.get('to', "now"))
        splits = [x.strip() for x in lc.get('splits', '').split(',') if x!='']
        aggexpr_list = [x.strip() for x in lc.get('agg', 'sum(v1)').split(',') if x!='']
        print splits, len(splits)
        print aggexpr_list, len(aggexpr_list)
        y2_flag = lc.get('y2', False)
        print 'y2: ', y2_flag
        if len(names)<len(aggexpr_list):
            for z in range(len(names), len(aggexpr_list)):
                names.append(event+str(i)+'_'+str(z))

        freq = lc.get('freq', None)
        tmp_anno_event = anno_event or event
        kwargs = {}
        for c in ['country', 'app']:
            if c in lc:
                kwargs[c] = [x.strip() for x in lc[c].split(',') if x!='']
        print kwargs

        for z, aggexpr in enumerate(aggexpr_list):
            try:
                sk = es_search_body(event=event, sd=sd, ed=ed, splits=splits, aggexpr=aggexpr, freq=freq, **kwargs)
                pprint.pprint(sk)
                agg_obj = es.search(index='ffskpi', doc_type='record', body=sk, filter_path=['aggregations'])
                #pprint.pprint(agg_obj['aggregations'])
                df = pd.DataFrame(es_flatten_aggs(agg_obj['aggregations']))
                print df.head().to_string()
                anno_entries = map(lambda x: x['_source'], list(eshelpers.scan(es, index='ffskpi', doc_type='annotation', query={"query": {
                                "filtered": {
                                    "filter": {
                                        "bool": {
                                            "must": [
                                                {"range" : {
                                                        "date" : {
                                                            "gte" : sd,
                                                            "lt" :  ed
                                                        }
                                                    }},
                                                {"wildcard": {
                                                    "event": tmp_anno_event 
                                                }}
                                            ]
                                        }
                                    }
                                }
                            }})))

                #print anno_entries
                
                partial_xys, partial_annos = extract_xy_anno(df.fillna(0), 'date', 'outcome', names[z], splits, anno_entries, 'comment')
                if y2_flag:
                    for p in partial_xys:
                        p['yaxis'] = 'y2'

                if y2_flag:
                    print 'check flag:'
                    print partial_xys[0]
                xys.extend(partial_xys)
                annos.extend(partial_annos)

            except:
                print 'Error in processing line', sys.exc_info()[1]

    annos = [dict(t) for t in set([tuple(sorted(d.items())) for d in annos])]
    pprint.pprint(annos)

    chart_obj = {}
    chart_obj['xys'] = xys
    chart_obj['annos'] = annos
    #chart_obj['grid'] = grid_config
    return chart_obj

#get max date/total doc count for each event in ffskpi
def es_event_max_date(es, index='ffskpi', doc_type='record'):
    md = es.search(index, doc_type, filter_path=['aggregations'], body={
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
        "aggs": {
            "event":{
                "terms": {"field": "event", "size": 0},
                "aggs": {
                    "max_date" : { "max" : { "field" : "date" } }
                }}
        }})

    ret = {}
    for x in md['aggregations']['event']['buckets']:
        tmp = {'max_date': x['max_date']['value_as_string'], 'size': x['doc_count']}
        ret[x['key']] = tmp
    
    return ret

def sql_to_es(es, sql, engine, event=None, index='ffskpi', doc_type="record"):
    accepted_columns = ['date', 'event', 'app', 'country', 'os', 'value', 'value2']
    dangerous_words = ['delete', 'truncate', 'insert', 'drop']
    sql = sql.replace(';', '')
    for dw in dangerous_words:
        sql = re.sub(dw, '', sql, flags=re.IGNORECASE)
    if event:
        ret = es.search(index=index, doc_type=doc_type, body={
        "query" : {
            "term" : { 
                        "event" : event
                    }
                },
          "sort": [
            {
              "date": {
                "order": "desc"
              }
            }
          ],
          "size": 1
        })['hits']['hits']
        
        if len(ret)>0:
            max_date = ret[0]['_source']['date']
            wrapped_sql = '''select * from ({sql}) where date+2>='{md}';'''.format(sql=sql, md=max_date)
        else:
            wrapped_sql = sql
            
        print wrapped_sql
    else:
        #todo (if needed): if multiple events specified in sql, do a limit query
        #then get last date of each event from ES
        wrapped_sql = sql
    
    try:
        df = pd.read_sql_query(wrapped_sql, engine)
    except sqlalchemy.exc.ProgrammingError:
        print sys.exc_info()[1]
        return
    
    if 'date' not in df.columns:
        print "Date must be a column of query result! Exiting"
        return
    else:
        try:
            df['date'] = pd.to_datetime(df['date']).map(lambda x: x.date())
        except:
            print "Failed to coerce date column to date type! Exiting..."
            return

    df = df[[x for x in df.columns if x in accepted_columns]] 
    if event:
        df['event'] = event
        
    if 'event' not in df.columns:
        print "You need to specify an event either in the sql or function argument! Exiting..."
        return
    if 'value' not in df.columns:
        print "You need to specify a value column in query result! Exiting..."
        return

    df['_id'] = df.apply(lambda x: hash(''.join([str(x[c]) for c in df.columns if not c.startswith('value')])), axis=1)
    objs = df.to_dict(orient='records')
    success, _ = bulk(es, objs, index=index, doc_type=doc_type, raise_on_error=True)
    print('Performed %d actions' % success)
    return success    