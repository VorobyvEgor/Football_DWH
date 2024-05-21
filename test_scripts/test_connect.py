import json
import psycopg2
import requests
import yaml
import configparser
from datetime import datetime


def print_res(res):
    print(
        f"GET: {res.json()['get']}"
        , f"PARAMETRES: {res.json()['parameters']}"
        , f"ERRROS: {res.json()['errors']}"
        , f"RESULTS: {res.json()['results']}"
        , f"PAGING: {res.json()['paging']}"
        , sep='\n')


def load_yaml_dict(yaml_path: str):
    with open(yaml_path, 'r') as f:
        output = yaml.safe_load(f)
    return output


def conn_to_pg():
    config = configparser.ConfigParser()
    config.read('../credentials.ini')

    dbname = config['postgre']['dbname']
    user = config['postgre']['user']
    password = config['postgre']['password']
    host = config['postgre']['host']
    port = config['postgre']['port']

    conn = psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port
    )

    return conn


def creds_for_football_api():
    import configparser

    config = configparser.ConfigParser()
    config.read('../credentials.ini')

    headers = {
        'x-rapidapi-host': config['api_football']['host'],
        'x-rapidapi-key': config['api_football']['key']
    }

    url = config['api_football']['url']

    return headers, url


def api_request(headers: dict, url: str, dop_url: str) -> dict:
    res = requests.request("GET", url=url + dop_url, headers=headers)
    print_res(res)

    return res.json()


def load_to_db(conn: psycopg2.connect, data: list, schema: str, table_name: str, truncate_flg=True) -> None:
    try:
        with conn:
            cursor = conn.cursor()
            if truncate_flg:
                cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
            for value in data:
                print(f'Load to DB: {value}')
                cursor.execute(f"""
                    INSERT INTO {schema}.{table_name}
                    VALUES ({'%s,'*(len(value.values()) + 1)} %s)
                """, tuple(value.values()) + (datetime.now(), 'API_FOOTBALL'))
        print("Load SUCCESS")
    except Exception as err:
        print(f"Unexpected {err=}, {type(err)=}")


def parse_list(keys_list: list, main_key: str, data: dict):
    exploded_dict = {}

    # print("parse_list function params: ", keys_list, main_key, data, sep='\n')
    for key in keys_list:
        exploded_dict[f'{main_key}_{key}'] = data[key]
    return exploded_dict


def parse_dict(key_response: str, yaml_dict, data: dict):

    # print("parse_dict attrubutes: ", f"key_response={key_response}", f"yaml_dict={yaml_dict}", f"data={data}", sep='\n')

    result_dict = {}
    if type(yaml_dict['keys']) != dict:
        return parse_list(keys_list=yaml_dict['keys'], main_key=key_response, data=data)
    else:
        for key in yaml_dict.keys():
            result_dict.update(parse_dict(key_response=key, yaml_dict=yaml_dict, data=data[key]))
    return result_dict


def get_list_request_result(query: str, specification: dict):

    headers, url = creds_for_football_api()

    dop_url = specification[query]['url']

    request = api_request(headers=headers, url=url, dop_url=dop_url)

    result_list = []
    for item in request['response']:
        result_list.append(parse_attribute(key_response=dop_url, config=specification[query]['parameters'], data=item))

    return result_list

def parse_attribute(key_response, config:dict,  data:dict):

    result_dict = {}

    if config['type'] == 'list':
        return parse_list(keys_list=config['attributes'], main_key=key_response, data=data)

    for key in config['attributes']:
        exploded_dict = {}
        attr = config['attributes'][key]
        if attr['type'] == 'list':
            result_dict.update(parse_list(keys_list=attr['keys'], main_key=key, data=data[key]))
        elif attr['type'] == 'array':
            array_attr = []
            array_dict = {}
            for item in data[key]:
                for k in attr['keys']:
                    array_dict[f'{k}'] = item[k]
                array_attr.append(json.dumps(array_dict))
            exploded_dict[f'{key_response}_{key}'] = array_attr
            result_dict.update(exploded_dict)
    return result_dict



if __name__ == '__main__':
    # with open('../football_api_request_attributes.yaml', 'r') as f:
    #     output = yaml.safe_load(f)
    #
    # print(output)
    # print(type(output['leagues']) == dict)
    # print(type(output['countries']))

    query_test = 'league'

    yaml_dict = load_yaml_dict('../football_api_request_attributes.yaml')

    table_name = yaml_dict[query_test]['table_name']
    schema_name = yaml_dict[query_test]['schema_name']

    data = get_list_request_result(query=query_test, specification=yaml_dict)

    conn = conn_to_pg()
    load_to_db(conn=conn, data=data, schema=schema_name, table_name=table_name)

    # headers, url = creds_for_football_api()
    #
    # request = api_request(headers=headers, url=url, dop_url='leagues')
    #
    # print(request['response'][0])
    #
    # # kyes_for_explode = ['league', 'country']
    # kyes_for_explode = request['response'][0].keys()
    # exploded_dict = {}
    # for key in kyes_for_explode:
    #     for key2 in request['response'][0][key].keys():
    #         exploded_dict[f'{key}_{key2}'] = request['response'][0][key][key2]
    #
    #
    # print(exploded_dict.values())
    #
    # with open('table_attrubutes.yaml', 'r') as f:
    #     output = yaml.safe_load(f)

    # headers, url = creds_for_football_api()
    # conn = conn_to_pg()
    #
    # request = api_request(headers=headers, url=url, dop_url='countries')
    #
    # load_to_db_countries(conn=conn, table=request, schema='api_football_first_load', table_name='countries'
    #                      , truncate_flg=True)
