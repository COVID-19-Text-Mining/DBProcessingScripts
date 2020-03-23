import json
import pymongo
import regex
from datetime import datetime

###########################################
# parse time data
###########################################
PATTERN_DATE_0 =[
    '%m/%d/%Y',
    '%Y-%m-%d',
    '%Y',
    '%Y %b %d',
    '%Y %b',
]

MONTH_DICT = {
    'jan': 1,
    'feb': 2,
    'mar': 3,
    'apr': 4,
    'may': 5,
    'jun': 6,
    'jul': 7,
    'aug': 8,
    'sep': 9,
    'oct': 10,
    'nov': 11,
    'dec': 12,
}
SEASON_DICT = {
    'spring': 1,
    'summer': 4,
    'autumn': 7,
    'fall': 7,
    'winter': 10,
}

PATTERN_DATE_1 = [
    regex.compile(
        '(?P<year>[0-9]{{4}}) +(?P<month>{}).*'.format(
            '|'.join(list(MONTH_DICT.keys()))
        )
    ),
    regex.compile(
        '(?P<year>[0-9]{{4}}) +(?P<season>{}).*'.format(
            '|'.join(list(SEASON_DICT.keys()))
        )
    ),
    regex.compile(
        '.*(?P<year>[0-9]{4})-(?P<month>[0-9]{1,2})-(?P<day>[0-9]{1,2}).*'
    ),
]

def parse_date(date_obj):
    date = {}
    if isinstance(date_obj, str):
        date = parse_date_str(date_obj)
    if isinstance(date_obj, list):
        date = parse_date_list(date_obj)
    if len(date) < 3:
        for k in ({'year', 'month', 'day'} - set(date.keys())):
            date[k] = None
    return date

def parse_date_list(date_list):
    time_parsed = {}
    if len(date_list) == 3 and len(date_list[0])==4:
        time_parsed = {
            'year': int(date_list[0]),
            'month': int(date_list[1]),
            'day': int(date_list[2]),
        }
    if len(date_list) == 2 and len(date_list[0])==4:
        time_parsed = {
            'year': int(date_list[0]),
            'month': int(date_list[1]),
        }
    return time_parsed

def parse_date_str(date_str):
    time_parsed = {}
    date_str = date_str.strip()
    for a_pattern in PATTERN_DATE_0:
        try:
            result = datetime.strptime(date_str, a_pattern)
            if '%Y' in a_pattern:
                time_parsed['year'] = result.year
            if '%m' in a_pattern or '%b' in a_pattern:
                time_parsed['month'] = result.month
            if '%d' in a_pattern:
                time_parsed['day'] = result.day
            break
        except:
            pass
    if len(time_parsed) == 0:
        for a_pattern in PATTERN_DATE_1:
            tmp_m = a_pattern.match(date_str.lower())
            if tmp_m:
                result = tmp_m.groupdict()
                if 'year' in result:
                    time_parsed['year'] = int(result['year'])
                if 'month' in result:
                    if result['month'] in MONTH_DICT:
                        time_parsed['month'] = int(MONTH_DICT[result['month']])
                    else:
                        time_parsed['month'] = int(result['month'])
                if 'season' in result:
                    time_parsed['month'] = int(SEASON_DICT[result['season']])
                if 'day' in result:
                    time_parsed['day'] = int(result['day'])
                break
    return time_parsed

###########################################
# communicate with mongodb
###########################################

def get_mongo_db(config_file_path):
    """
    read config file and return mongo db object

    :param config_file_path: path to config file. config is a dict of dict as
                config = {
                'mongo_db': {
                    'host': 'mongodb05.nersc.gov',
                    'port': 27017,
                    'db_name': 'COVID-19-text-mining',
                    'username': '',
                    'password': '',
                }
            }
    :return:
    """
    with open(config_file_path, 'r') as fr:
        config = json.load(fr)

    client = pymongo.MongoClient(
        host=config['mongo_db']['host'],
        port=config['mongo_db']['port'],
        username=config['mongo_db']['username'],
        password=config['mongo_db']['password'],
        authSource=config['mongo_db']['db_name'],
    )
    db = client[config['mongo_db']['db_name']]
    return db