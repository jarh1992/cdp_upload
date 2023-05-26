#!/usr/bin/python3
"""
    cdp bases upload script
"""

import json
import datetime
from urllib.parse import urlparse
import asyncio
from functools import wraps
from asyncio.proactor_events import _ProactorBasePipeTransport
import platform
import pandas as pd
import aiohttp


def selc(func):
    """silence event loop closed"""
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except RuntimeError as rerr:
            if str(rerr) != 'Event loop is closed':
                raise
        return None
    return wrapper


if platform.system() == 'Windows':
    # Silence the exception here.
    _ProactorBasePipeTransport.__del__ = selc(_ProactorBasePipeTransport.__del__)


with open("assets/COUNTRY_ZONE_MAPPING.json", encoding="utf-8") as f:
    COUNTRY_ZONE_MAPPING = json.load(f)
with open("assets/BRANDS.json", encoding="utf-8") as f:
    BRANDS = json.load(f)
with open("assets/ABI_DATA.json", encoding="utf-8") as f:
    DATA = json.load(f)
with open("assets/CURRENCIES.json", encoding="utf-8") as f:
    CURRENCIES = json.load(f)
with open("assets/API_KEYS.json", encoding="utf-8") as f:
    API_KEYS = json.load(f)



def get_db():
    """
        get DB (golden or diamond records)
        return:
            0: Diamond records
            1: Golden records
    """
    print('[0] Diamond_records \n[1] Golden_records')
    db_num = -1
    while db_num not in (0, 1):
        db_num = int(input("\nSelect environment by num: "))
    print('Selected', 'Golden_records' if db_num else 'Diamond_records')

    return db_num


def get_env():
    """
        get environment (dev or prod)
        return boolean: False: dev, True: Prod
    """
    print('[0] dev \n[1] Prod')
    env_num = -1
    while env_num not in (0, 1):
        env_num = int(input("\nSelect environment by num: "))
    print('Selected', 'prod' if env_num else 'dev')
    return bool(env_num)


def get_csv() -> pd.DataFrame:
    """get and load CSV file"""
    file_path = input("path of CSV file (path + file_name.csv) ('|' pipe separated) (empty if not): ")
    try:
        csv = pd.read_csv(file_path, sep="|")
        print(f"Selected {file_path}")
        print("-"*72)
        return csv
    except Exception as exc:
        print(str(exc), "There is a problem with the file")
        return pd.DataFrame()
    #return None


def select_fields(daf):
    """select fields from ABI_DATA"""
    data_serie = pd.Series(DATA)
    while True:
        i = 0
        for key, val in data_serie.items():
            print(f'[{i}] {key}')
            i += 1
        num_data_items = len(data_serie)

        for col in daf.columns:
            field = -1
            while not 0 <= field < num_data_items:
                field = int(input(f"Please assign to **{col}** a field by num: "))

            i = 0
            for key, val in data_serie.items():
                if i==field:
                    data_serie[key] = col
                    break
                i += 1
        fields_selected = data_serie[data_serie!="---"].to_dict()

        print("You have setted all columns in this way:")
        for key, val in fields_selected.items():
            print(f'{key}: {val}')

        resp = input("Are you sure? [1] Yes: ")
        if resp == "1":
            print("-"*72)
            return fields_selected


def get_country():
    """get country"""
    i = 0
    zone = ""
    for key, val in COUNTRY_ZONE_MAPPING.items():
        if zone != val[-1]:
            if zone == "":
                zone = val[-1]
                print(f'----- {zone} -----')
            else:
                zone = val[-1]
                print(f'\n\n----- {zone} -----')
        print(f'[{i}] {key}', end='\t')
        i += 1

    country_num = -1
    czm = list(COUNTRY_ZONE_MAPPING.keys())
    while not 0 <= country_num < len(czm):
        country_num = int(input("\n\nSelect country by num: "))

    cntr = czm[country_num]
    print(f'Selected {cntr}')
    print("-"*72)
    return cntr


def get_brand():
    """get brand"""
    for idx, val in enumerate(BRANDS):
        print(f'[{idx}] {val}', end='\n' if idx%4==0 else '\t')

    brand_num = -1
    while not 0 <= brand_num < len(BRANDS):
        brand_num = int(input("\nSelect brand by num: "))

    brnd = BRANDS[brand_num]
    print(f'Selected {brnd}')
    print("-"*72)
    return BRANDS[brand_num]


async def send_data_G(session, form_data, country, brand, campaign, unify, env, url, idx):
    """send data to cdp api Golden records"""
    form_data['abi_brand'] = brand
    form_data['abi_campaign'] = campaign
    form_data['abi_form'] = campaign
    form_data['td_import_method'] = 'postback-api-1.2'
    form_data['td_unify'] = unify
    form_data['td_client_id'] = ''
    form_data['td_url'] = url
    form_data['td_host'] = urlparse(url).netloc

    td_env = 'prod' if env else 'dev'
    td_apikey = API_KEYS["g_" + td_env]
    td_zone = COUNTRY_ZONE_MAPPING[country][-1]

    api_url = f"https://in.treasuredata.com/postback/v3/event/{td_zone}_source/{country}_web_form"

    headers = {
        'X-TD-Write-Key': td_apikey,
        'Content-Type': 'application/json'
    }

    async with session.post(api_url, headers=headers, json=form_data) as resp:
        with open(f"status_{campaign}.txt", "a") as file:
            print(f'{datetime.datetime.now()} cdp_push send: {brand}-{campaign}: {idx}')
            file.write(f'{datetime.datetime.now()} cdp_push send: {brand}-{campaign}: {idx}')
            print(form_data)
            file.write(json.dumps(form_data))
            return idx, resp

    return None


async def send_data_D(session, form_data, country, brand, env, table, idx):
    """send data to cdp api Golden records"""
    td_env = 'prod' if env else 'dev'
    td_apikey = API_KEYS["g_" + td_env]
    td_zone = COUNTRY_ZONE_MAPPING[country][-1]

    api_url = f"https://in.treasuredata.com/postback/v3/event/global_ecommerce_postback/{td_zone}_{country}_{brand}_{table}"

    headers = {
        'X-TD-Write-Key': td_apikey,
        'Content-Type': 'application/json'
    }

    async with session.post(api_url, headers=headers, json=form_data) as resp:
        with open(f"status_{brand}_{table}.txt", "a") as file:
            print(f'{datetime.datetime.now()} cdp_push send: {brand}-{table}: {idx}')
            file.write(f'{datetime.datetime.now()} cdp_push send: {brand}-{table}: {idx}')
            print(form_data)
            file.write(str(form_data) + "\n")
            return idx, resp

    return None


async def main_request_G(dataf, fields, country, brand, campaign, url, env):
    """async method to send data to Golden records"""
    async with aiohttp.ClientSession() as session:
        tasks = []
        for row in dataf.itertuples():
            data = {k: getattr(row, v) for k,v in fields.items()}
            data["abi_country"] = country
            purposes = ['TC-PP']

            if "abi_marketingactivation" in data:
                if data.get("abi_marketingactivation"):
                    purposes.append('MARKETING-ACTIVATION')
                del data["abi_marketingactivation"]

            data['purpose_name'] = purposes
            tasks.append(asyncio.ensure_future(send_data_G(session, data, country, brand, campaign, True, env, url, row.Index)))

        td_response = await asyncio.gather(*tasks)
        with open(f"log_{campaign}.txt", "w") as file:
            for tdr in td_response:
                file.write(f'{datetime.datetime.now()} cdp_push send: {brand}-{campaign}: {tdr[0]}\n')
                file.write(f'\t\t{tdr[1].status}\n') #aiohttp
                file.write(f'\t\t{tdr[1].headers}\n') #aiohttp
                #file.write(f'\t\t{td_response.status_code}\n') #requests
                #file.write(f'\t\t{td_response.request.headers}\n') #requests


async def main_request_D(dt_c, dt_o, fields_c, fields_o, country, brand, env):
    """async method to send data"""
    async with aiohttp.ClientSession() as session:
        tasks_c = []    #task consumer table
        tasks_o = []    #task order table

        if not dt_c.empty:
            for row in dt_c.itertuples():
                data_c = {k: getattr(row, v) for k,v in fields_c.items()}

                tasks_c.append(asyncio.ensure_future(send_data_D(
                    session,
                    data_c,
                    country,
                    brand.lower().replace(' ',''),
                    env,
                    "customers",
                    row.Index
                )))

        if not dt_o.empty:
            for row in dt_o.itertuples():
                data_o = {k: getattr(row, v) for k,v in fields_o.items()}

                tasks_o.append(asyncio.ensure_future(send_data_D(
                    session,
                    data_o,
                    country,
                    brand,
                    env,
                    "orders",
                    row.Index
                )))

        td_response_c = await asyncio.gather(*tasks_c)
        td_response_o = await asyncio.gather(*tasks_o)
        with open("log_c.txt", "w") as file:
            for tdr in td_response_c:
                file.write(f'{datetime.datetime.now()} cdp_push send: {brand}-cust: {tdr[0]}\n')
                file.write(f'\t\t{tdr[1].status}\n')
                file.write(f'\t\t{tdr[1].headers}\n')
        with open("log_o.txt", "w") as file:
            for tdr in td_response_o:
                file.write(f'{datetime.datetime.now()} cdp_push send: {brand}-ord: {tdr[0]}\n')
                file.write(f'\t\t{tdr[1].status}\n')
                file.write(f'\t\t{tdr[1].headers}\n')


def main():
    try:
        DB = get_db()
        ENV = get_env()
        DB_MSG = "Golden records" if DB else "Diamond records"
        ENV_MSG = "PROD" if ENV else "DEV"
        print(f'{"-" * 53} {DB_MSG} {ENV_MSG} {"-" * 53}')

        if DB:
            #load csv
            df = get_csv()
            fields = select_fields(df)
            country = get_country()
            brand = get_brand()
            campaign = input("Insert campaign: ")
            url = input("Insert url: ")
            launching = input(f"Launch over DB {DB_MSG} and Env. {ENV_MSG}?: [1] Yes: ")
            if launching == "1":
                asyncio.run(main_request_G(df, fields, country, brand, campaign, url, ENV))
        else:
            #load csv
            country = get_country()
            brand = get_brand()
            df_c, df_o = pd.DataFrame(), pd.DataFrame()

            c_opt = input("send to customers?: [1] Yes:")
            if c_opt == "1":
                print("Customers CSV Selection")
                df_c = get_csv()
                fields_c = select_fields(df_c)

            o_opt = input("send to orders?: [1] Yes:")
            if o_opt == "1":
                print("Orders CSV Selection")
                df_o = get_csv()
                fields_o = select_fields(df_o)

            if df_c.empty and df_o.empty:
                pass
            else:
                launching = input(f"Launch over DB {DB_MSG} and Env. {ENV_MSG}?: [1] Yes: ")
                if launching == "1":
                    asyncio.run(main_request_D(df_c, df_o, fields_c, fields_o, country, brand, ENV))

        input("Finished. Press Enter to exit")
    except Exception as exp:
        print(str(exp))


if __name__ == '__main__':
    main()
