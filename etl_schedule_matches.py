import requests
from datetime import datetime, timedelta
import pandas as pd

from sqlalchemy import create_engine, text
import json

# Request to collect schedule
reqUrl = "https://prod-relapi.ewp.gg/persisted/gw/getSchedule"
headersList = {
 "Accept": "*/*",
 "User-Agent": "Thunder Client (https://www.thunderclient.com)",
 "x-api-key": "0TvQnueqKa5mxJntVWt0w4LpLfEkrV1Ta8rQBb9Z",
 "hl": "en-US" 
}
payload = ""

# Telegram Tokens
bot_token = '5536751753:AAHTXih_dMUWp9VhlT_C9myQdfkZNeY2XzU'
chat_id = "-1001757502512"

main_path = "."

with open(f'{main_path}/config.json') as f:
    config = json.load(f)

def get_data():
    
    msg = f"{datetime.now()} ---->>>> Collecting Data.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)

    response = requests.request("GET", reqUrl, data=payload,  headers=headersList)

    scheduled_matches = response.json()['data']['schedule']['events']

        
    msg = f"{datetime.now()} ---->>>> Data Collected.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)

    return scheduled_matches


def transform_schedule(scheduled_matches):

    
    msg = f"{datetime.now()} ---->>>> Transforming Data.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)

    today_scheduled_matches = []

    for match in scheduled_matches:
        match_info = {}

        if match['state'] == 'completed' or match['type'] != "match":
            continue

        start_time = datetime.strptime(match['startTime'], "%Y-%m-%dT%H:%M:%SZ")
        match_time_dt = start_time - timedelta(hours=3)

        # if datetime.now().date() != match_time_dt.date():
        #     continue

        if match['league']['name'] not in config['selected_leagues']:
            continue

        match_info['start_date'] = match_time_dt#.strftime('%Y-%m-%d %H:%M:%S')
        match_info['league'] = match['league']['name']
        match_info['match_type'] = f"{match['match']['strategy']['type']}_{match['match']['strategy']['count']}"
        match_info['block_name'] = match['blockName']

        match_info['match_id'] = match['match']['id']

        match_info['team_a_code'] = match['match']['teams'][0]['code']
        match_info['team_a_name'] = match['match']['teams'][0]['name']

        match_info['team_b_code'] = match['match']['teams'][1]['code']
        match_info['team_b_name'] = match['match']['teams'][1]['name']

        today_scheduled_matches.append(match_info)

    msg = f"{datetime.now()} ---->>>> Data Transformed.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)

    data_transformed_df = pd.DataFrame(today_scheduled_matches)

    return data_transformed_df


def create_conn():
        
    config = {
        'host': 'isabelle.db.elephantsql.com',
        'port': '5432',
        'database': 'ngrnlmey',
        'username': 'ngrnlmey',
        'password': 'X_8oCldAPtAbKn78Bxo7f7B9kVKIQldc'
    }

    url = f"postgresql://{config['username']}:{config['password']}@{config['host']}:{config['port']}/{config['database']}"

    print(f"{datetime.now()} ---->>>> Salvando tabela no postgres.\n")
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(f"{datetime.now()} ---->>>> Salvando tabela no postgres.\n")

    print(f"{datetime.now()} ---->>>> Criando Engine.\n\n\n")
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(f"{datetime.now()} ---->>>> Criando Engine.\n\n\n")
    engine = create_engine(url)

    print(f"{datetime.now()} ---->>>> Engine Criada.\n\n\n")
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(f"{datetime.now()} ---->>>> Engine Criada.\n\n\n")

    conn =  engine.connect()

    return conn, engine


def save_schedule(scheduled_matches_df, conn, engine):

    msg = f"{datetime.now()} ---->>>> Saving Data.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)


    scheduled_matches_df.to_sql('lol_schedule', engine, chunksize=10000, if_exists='replace', index=False)

    conn.close()

    msg = f"{datetime.now()} ---->>>> Data Saved.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)


def send_schedule(scheduled_matches_df):

    msg = f"{datetime.now()} ---->>>> Sending Data.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)

    message = f"""
    *******************************
    Today's Matches ({datetime.now().strftime('%Y-%m-%d')})
    *******************************
    """
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}"
    requests.get(url).json()

    for index, row in scheduled_matches_df.iterrows():

        if row['start_date'].date() != datetime.now().date():
            continue

        message = f"""
        {row['team_a_code']} ({row['team_a_name']}) x {row['team_b_code']} ({row['team_b_name']})
        League: {row['league']}
        MatchType: {row['match_type']}
        BlockName: {row['block_name']}
        StartTime: {row['start_date']}
        """
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage?chat_id={chat_id}&text={message}"
        requests.get(url).json()

    msg = f"{datetime.now()} ---->>>> Data Sent.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)


if __name__ == '__main__':

    msg = f"{datetime.now()} ---->>>> Iniciando o projeto.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)

    scheduled_matches = get_data()

    scheduled_matches_df = transform_schedule(scheduled_matches)

    conn, engine = create_conn()

    save_schedule(scheduled_matches_df, conn, engine)

    send_schedule(scheduled_matches_df)

    msg = f"{datetime.now()} ---->>>> Projeto Finalizado.\n"
    print(msg)
    with open(f'{main_path}/logs.txt', 'a') as f:
        f.write(msg)
