import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import io
import sys
import os
import pandas as pd
import pandahouse
from datetime import datetime, timedelta, date

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


class Getch:
    def __init__(self, query, db='simulator'):
        self.connection = {
            'host': 'https://clickhouse.lab.karpov.courses',
            'password': 'dpo_python_2020',
            'user': 'student',
            'database': db,
        }
        self.query = query
        self.getchdf
    @property
    def getchdf(self):
        try:
            self.df = pandahouse.read_clickhouse(self.query, connection=self.connection)
        except Exception as err:
            print("\033[31m {}".format(err))
            exit(0)

# Дефолтные параметры, которые прокидываются в таски
default_args = {
    'owner': 'e-koteljanets',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 6, 7),
}

schedule_interval = '*/15 * * * *' 

TOKEN = '6219304136:AAHUr8dQC9QVzK8FWYJzCBIdLoL7gVKULeY'
chat_id = '-938659451'

# Для ленты
query_f = '''
        SELECT toStartOfFifteenMinutes(time) as ts,
               toDate(time) as date,
               formatDateTime(ts, '%R') as hm,
               uniqExact(user_id) as users_feed,
               countIf(action = 'view') as views,
               countIf(action = 'like') as likes,
               ROUND(countIf(user_id, action='like') / countIf(user_id, action='view'), 2) AS ctr
        FROM simulator_20230520.feed_actions
        WHERE time >= today() - 1 AND time < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts'''
# Для сообщений
query_m = ''' 
        SELECT
            toStartOfFifteenMinutes(time) as ts, -- преобразовывает дату к ближайщей 15 минутке
            toDate(ts) as date,
            formatDateTime(ts, '%R') as hm, -- преобразовывает дату по 15 минутке
            count(distinct user_id) as users_mess,
            count(reciever_id) as messages_sent
        FROM {db}.message_actions
        WHERE ts >=  today() - 1 and ts < toStartOfFifteenMinutes(now())
        GROUP BY ts, date, hm
        ORDER BY ts'''

# Листы метрик
metrics_feed = ['users_feed', 'views', 'likes', 'ctr']
metrics_messages = ['users_mess','messages_sent']

@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def alert_sys_kotelyanets():

    def check_anomaly(df, metric, a = 4, n = 5):
        # функция предлагает алгоритм поиска аномалий (межквартильный размах)
        df['q25'] = df[metric].shift(1).rolling(n).quantile(0.25)
        df['q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
        df['iqr'] = df['q75'] - df['q25']
        df['up'] = df['q75'] + a*df['iqr']
        df['low'] = df['q25'] - a*df['iqr']

        df['up'] = df['up'].rolling(n, center=True, min_periods=1).mean()
        df['low'] = df['low'].rolling(n, center=True, min_periods=1).mean()

        if df[metric].iloc[-1] < df['low'].iloc[-1] or df[metric].iloc[-1] > df['up'].iloc[-1]:
            is_alert = 1
        else:
            is_alert = 0

        return is_alert, df


    @task()
    def run_alerts(chat_id = None, metrics_list = None, query = None):
        # Система алертов
        chat_id = chat_id or 1069207675
        bot = telegram.Bot(token = TOKEN)
        
        data = Getch(query).df
        metrics_list = metrics_list
        
        for metric in metrics_list:
            df = data[['ts', 'date', 'hm', metric]].copy()
            is_alert, df = check_anomaly(df, metric)
            if is_alert == 1:
                current_val = df[metric].iloc[-1]
                last_val_diff = abs(1- (df[metric].iloc[-1]/df[metric].iloc[-2]))
                df[metric].iloc[-2]                     
                msg = f'''🆘Метрика: {metric}❗️
@heka_k, посмотри
Время: {datetime.now().strftime("%H.%M.%S")}
______________________________
Текущее значение: {current_val:.2f}
Отклонение от предыдущего значения {last_val_diff:.2%}
Ссылка на дашборд: https://superset.lab.karpov.courses/superset/dashboard/3722/'''
                sns.set(rc={'figure.figsize':(16, 10)})
                plt.tight_layout()
                ax = sns.lineplot(x = df['ts'], y = df[metric], label = metric)
                ax = sns.lineplot(x = df['ts'], y = df['up'])
                ax = sns.lineplot(x = df['ts'], y = df['low'])

                for ind, label, in enumerate(ax.get_xticklabels()):
                    if ind % 2 == 0:
                        label.set_visible(True)
                    else:
                        label.set_visible(False)
                ax.set(xlabel = 'time')
                ax.set(ylabel = metric)

                ax.set_title(metric)
                ax.set(ylim = (0, None))
                plot_object = io.BytesIO()
                plt.savefig(plot_object)
                plot_object.seek(0)
                plot_object.name = '{0}.png'.format(metric)
                plt.close()
                
                bot.sendMessage(chat_id = chat_id, text = msg)
                bot.sendPhoto(chat_id = chat_id, photo = plot_object)
    
            return
    
    alert_f = run_alerts(chat_id = chat_id, metrics_list = metrics_feed, query = query_f)
    alert_m = run_alerts(chat_id = chat_id, metrics_list = metrics_messages, query = query_m)

    
alert_sys_kotelyanets = alert_sys_kotelyanets()

































