# 테스트용
from pickle import FALSE
import win32com.client
import pythoncom
import os
import json
import datetime
import time

import win32com
import xing_login
from xing_methods import getJongmokCodeList, getFullPriceData, getJongmokName
import event_logger as log

data_path = 'data/price'
os.makedirs(data_path, exist_ok=True)
# "jongmok_code": "000040",
# "jongmok_name": "KR���ͽ�",
# "corp_WICS": "�ڵ���",
# "financial_info": {


class Scheme:
    price = {'date': str, 'open': int,
             'close': int, 'high': int, 'low': int, 'mod_rate': float}  # 추가 정보 있으나 의미 없는 것으로 보임.
    price_data = {'jongmok_code': str, 'jongmok_name': str,
                  'is_delisted': bool, 'start_date': str, 'end_date': str, 'date_idx': {'date: idx'}, 'price_info': [price]}


class PriceAgent:
    def __init__(self):
        xing_login.logIn()

    def _getModRate(self, price_past, price):
        mod_rate = price['open']/price_past['close']
        if mod_rate < 1/1.5:
            log.write('stock division detected:', price)
            return mod_rate
        return 1

    def _addModRate(self, price_info, start_idx=0):
        price_past = ''
        if start_idx == 0:
            mod_rate_past = 1
        else:
            mod_rate_past = price_info[start_idx-1]['mod_rate']
        for i in range(start_idx, len(price_info)):
            if i == start_idx:
                mod_rate = mod_rate_past
            else:
                mod_rate = mod_rate_past * \
                    self._getModRate(price_past, price_info[i])
            price_info[i]['mod_rate'] = mod_rate
            price_past = price_info[i]
            mod_rate_past = mod_rate
        return price_info

    def _create(self, jongmok_code, start_date, end_date):
        price_data = {'jongmok_code': jongmok_code, 'jongmok_name': getJongmokName(
            jongmok_code), 'is_delisted': False}
        val, price_info, date_idx = getFullPriceData(
            jongmok_code, start_date, end_date)
        print(price_info)
        if val:
            price_data['start_date'] = price_info[0]['date']
            price_data['end_date'] = price_info[-1]['date']
            price_data['date_idx'] = date_idx
            price_data['price_info'] = self._addModRate(price_info)
        # log.write(price_data)
        return price_data

    def _update(self, jongmok_code, price_data,  end_date=-1):
        if end_date == -1:
            end_date = self._today()
        start_date = self._nextdate(price_data['end_date'])
        to_append = price_data['price_info']
        start_idx = len(to_append)
        print(start_date, end_date, to_append)
        val, price_info, date_idx = getFullPriceData(
            jongmok_code, start_date, end_date, to_append, price_data['date_idx'])
        print(val, price_info)
        if val:
            price_data['end_date'] = price_info[-1]['date']
            price_data['date_idx'] = date_idx
            price_data['price_info'] = self._addModRate(price_info, start_idx)
        # log.write(price_data)
        return price_data

    def _nextdate(self, date):
        date = int(date)
        y, m, d = date//10000, (date % 10000)//100, date % 100
        new_date = datetime.date(y, m, d)
        new_date += datetime.timedelta(days=1)
        return str(new_date).replace('-', '')

    def _today(self):
        date = datetime.date.today()
        return str(date).replace('-', '')

    def _delist(self, jongmok_code, price_data):
        price_data['is_delisted'] = True
        return price_data

    def save(self, jongmok_code):
        data_name = '{}/{}.json'.format(data_path, jongmok_code)
        if os.isfile(data_name):
            with open(data_name, "r") as json_file:
                to_append = json.load(json_file)


if __name__ == '__main__':
    price_agent = PriceAgent()
    data = price_agent._create('035720', '20210801', '20210805')
    time.sleep(3)
    data3 = price_agent._create('078020', '20210801', '20210806')
    # print(data)
    time.sleep(3)
    data2 = price_agent._update('035720', data)
    # print(data2)
