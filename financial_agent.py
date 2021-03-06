# -*-coding: utf-8
# from request_wrapper import wrapped_requests as requests
# from dart_crawler import DartCrawler
from utils import removeEmpty

# from selenium.webdriver.common.alert import Alert
from datetime import datetime, timedelta
import xml.etree.ElementTree as ET
from selenium import webdriver
from bs4 import BeautifulSoup
from copy import deepcopy
import configparser
import subprocess
import datetime
import openpyxl
import sqlite3
import pickle
import random
import xlrd
import time
import json
import glob
import sys
import re
import os

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# 입력값
filepath_financial = 'financial'
error_code_list = []


class FinancialAgent:
    url = 'https://kind.krx.co.kr/corpgeneral/corpList.do?method=loadInitPage'
    financial_info_keys_kor = ['매출액', '영업이익', '영업이익(발표기준)', '세전계속사업이익', '당기순이익', '당기순이익(지배)', '당기순이익(비지배)', '자산총계', '부채총계', '자본총계', '자본총계(지배)', '자본총계(비지배)', '자본금', '영업활동현금흐름', '투자활동현금흐름', '재무활동현금흐름', 'CAPEX', 'FCF', '이자발생부채', '영업이익률', '순이익률', 'ROE(%)', 'ROA(%)', '부채비율', '자본유보율', 'EPS(원)', 'PER(배)', 'BPS(원)', 'PBR(배)', '현금DPS(원)', '현금배당수익률', '현금배당성향(%)', '발행주식수(보통주)'
                               ]
    financial_info_keys_eng = ['sales', 'op_income', 'op_income_when_announced', 'BTCF', 'net_income', 'net_income_dom', 'net_income_non_dom', 'asset', 'debt', 'equity', 'equity_dom', 'equity_non_dom', 'capital_stock', 'op_CF', 'inv_CF', 'fin_CF', 'CAPEX', 'FCF', 'debt_with_interest', 'op_income_r', 'income_r', 'ROE', 'ROA', 'debt_r', 'reserve_r', 'EPS', 'PER', 'BPS', 'PBR', 'cash_DPS', 'cash_dividend_yield', 'dividend_payout_r', 'tot_issued_stock'
                               ]
    financial_info_value_type = ["int", "int", "int", "int", "int", "int", "int", "int", "int", "int", "int", "int", "int", "int", "int", "int",
                                 "int", "int", "int", "float", "float", "rate", "rate", "float", "float", "int", "float", "int", "float", "int", "float", "rate", "int"]
    invest_info_keys_kor = ["시가총액", "시가총액순위", "상장주식수", "액면가", "매매단위", "외국인한도주식수", "외국인보유주식수", "외국인소진율", "투자의견",
                            "목표주가", "52주최고", "최저", "PER", "EPS", "추정PER", "EPS", "PBR", "BPS", "배당수익률", "", "PER", "등락률"]
    invest_info_keys_eng = ["market_cap", "market_cap_rank", "stock_number", "face_price", "trading_unit", "foreigner_limit", "foreigner_held", "foreigner_burnout",
                            "opinion", "target_price", "52_high", "52_low", "PER", "EPS", "estimated_PER", "estimated_EPS", "PBR", "BPS", "yield", "", "industry_PER", "industry_rate"]
    invest_info_value_type = ["억원", "int", "int", "int", "int", "int", "int", "rate", "signed",
                              "int", "int", "int", "float", "int", "float", "int", "float", "int", "rate", "float", "rate signed"]

    corp_filter_list = ["전기제품",
                        "컴퓨터와주변기기",
                        "반도체와반도체장비",
                        "기타금융",
                        "우주항공과국방",
                        "상업서비스와공급품",
                        "기계",
                        "무역회사와판매업체",
                        "창업투자",
                        "자동차",
                        "철강",
                        "건강관리기술",
                        "양방향미디어와서비스",
                        "생물공학",
                        "항공사",
                        "가정용기기와용품",
                        "소프트웨어",
                        "전기장비",
                        "레저용장비와제품",
                        "운송인프라",
                        "독립전력생산및에너지거래"]

    def __init__(self, echo=True, echo_error=True):
        self.jongmok_code = ""
        self.access_url = ""
        self.echo = echo
        self.echo_error = echo_error
        self.save_dir = f'{BASE_DIR}/data/financial'
        if not os.path.isdir(self.save_dir):
            os.makedirs(self.save_dir)
        self.save_code_list_path = f'{self.save_dir}/jongmok_code_list.json'
        self.log_path = f'{self.save_dir}/financial_filter_log.json'
        if not os.path.isfile(self.save_code_list_path):
            self.log("create new jongmok code list file.")
            with open(self.save_code_list_path, "w", encoding='utf-8') as json_file:
                json.dump({"create_date": datetime.date.today().strftime(
                    "%Y%m%d")}, json_file, indent=4, ensure_ascii=False)
        if not os.path.isfile(self.log_path):
            self.log("create new log file.")
            with open(self.log_path, "w", encoding='utf-8') as json_log_file:
                json.dump({"create_date": datetime.date.today().strftime(
                    "%Y%m%d"), "log": []}, json_log_file,
                    indent=4, ensure_ascii=False)

        options = webdriver.ChromeOptions()
        options.add_argument('headless')
        self.driver = webdriver.Chrome(
            './chromedriver/chromedriver', options=options)
        self.driver.implicitly_wait(3)
        self.log("successfully initialized financial agent.")

    def log(self, content):
        if self.echo:
            print('FinancialAgent> \033[92m{}\033[0m'.format(content))

    def errlog(self, content):
        if self.echo_error:
            print('FinancialAgent> \033[91m{}\033[0m'.format(content))

    def _writeFinancialLog(self, state=404, err_type="NORMAL", err_detail={}):
        if err_type in ["INVALID_VAL", "INVALID_VAL_INT", "INVALID_VAL_FLOAT", "MISSING_VAL", ]:
            return -1
        # state: 1(BUY) / 0(DELAY) / -1(DO NOT BUY) / 404(FAIL)
        # err_type: NORMAL / INVALID_CODE / UNKNOWN_ERR / INVALID_VAL / INVALID_VAL_INT / INVALID_VAL_FLOAT / MISSING_INFO / MISSING_VAL / MISSING_DATA
        with open(self.log_path, "r", encoding='utf-8') as json_file:
            json_data = json.load(json_file)
        json_data["log"].append({"date": datetime.date.today().strftime(
            "%Y%m%d"), "jongmok_code": self.jongmok_code, "access_url": self.access_url, "state": state, "err_type": err_type, "err_detail": err_detail})
        with open(self.log_path, "w", encoding='utf-8') as json_file:
            json.dump(json_data, json_file, indent=4, ensure_ascii=False)

    def _getPageSource(self, type="wisereport"):
        if (type == 'wisereport') or (type == 'quarter'):
            self.access_url = f'https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd={self.jongmok_code}'
        elif type == 'financial':
            self.access_url = f'https://finance.naver.com/item/coinfo.naver?code={self.jongmok_code}'
        self.log('getting page source.')
        self.log('target url : {}'.format(self.access_url))
        try:
            time.sleep(1)
            self.driver.get(self.access_url)
            self.driver.implicitly_wait(1)
            if type == 'wisereport':
                self.driver.find_element_by_id('cns_td21').click()
            elif type == 'quarter':
                self.driver.find_element_by_id('cns_td22').click()
            page = self.driver.page_source
            return True, page
        except Exception as e:
            if "올바른 종목이 아닙니다" in e.args[0]:
                self._writeFinancialLog(
                    err_type="INVALID_CODE", err_detail={"args": e.args})
                self.errlog('invalid jongmok code.')
                self.errlog(e.args[0])
            else:
                self._writeFinancialLog(
                    err_type="UNKNOWN_ERR", err_detail={"args": e.args})
                self.errlog('unknown error occured.')
                self.errlog(e.args[0])
            return False, ""

    def _translator(self, name):
        return self.financial_info_keys_eng[self.financial_info_keys_kor.index(name)]

    '''
    def _yearMonthMaker(self, string, type="year"):
        if type == "year":
            data = string.split('/')[0]
        elif type == "quarter":
            temp = string.split('/')
            data = temp[0] + temp[1].split('(')[0]
        if 'E' in string:
            return data + 'E'
        else:
            return data
    '''

    def _yearMonthMaker(self, string, type="year"):
        if type == "year":
            data = string.split('/')[0]
        elif type == "quarter":
            temp = string.split('/')
            if len(string) == 0:
                return ''
            data = temp[0] + temp[1].split('(')[0]

        if 'E' in string:
            return data + 'E'
        else:
            return data

    def _findLatest(self, path):
        file_name_and_time_list = []
        for f_name in os.listdir(f"{path}"):
            written_time = os.path.getctime(f"{path}/{f_name}")
            file_name_and_time_list.append((f_name, written_time))
        sorted_file_list = sorted(
            file_name_and_time_list, key=lambda x: x[1], reverse=True)
        recent_file = sorted_file_list[0]
        return path + '\\' + recent_file[0]

    '''
    def _updateFinancial(self, dict_input, type="year"):
        # if 'year_latest' in dict_input.keys():
        #     if dict_input['year_latest']+1 >= datetime.date.today().year:
        #         return False, dict_input
        if type == "year":
            result, page = self._getPageSource(self.jongmok_code)
        elif type == "quarter":
            result, page = self._getPageSource(self.jongmok_code, "quarter")

        if result == False:
            return False, dict_input

        soup = BeautifulSoup(page, 'html.parser')
        table_list = soup.select('table.gHead01')
        table_item = None
        for table in table_list:
            caption = removeEmpty(table.select('caption.blind')[0].text)
            if "주요재무정보" == caption:
                table_item = table
                break
        if table_item is None:
            return False, dict_input

        jongmok_name = soup.select('span.name')[0].text
        corp_WICS = removeEmpty(soup.select('td.td0101')[0].select(
            'dt.line-left')[2].text.split(':')[1])
        dict_input['jongmok_code'] = self.jongmok_code
        dict_input['jongmok_name'] = jongmok_name
        dict_input['corp_WICS'] = corp_WICS
        if 'financial_info' not in dict_input.keys():
            dict_input['financial_info'] = {}

        tr_list = table_item.select('thead > tr')  # 표의 제목 부분
        year_list = []
        for tr_idx, tr_item in enumerate(tr_list):
            th_list = tr_item.select('th')
            if tr_idx == 0:
                continue
            else:
                for th_idx in range(len(th_list)):
                    try:
                        year_list.append(self._yearMonthMaker(
                            removeEmpty(th_list[th_idx].text), type))
                    except:
                        continue

        year_latest_updated = False
        is_year = False
        for idx, year in enumerate(year_list):
            if year not in dict_input['financial_info'].keys():
                dict_input['financial_info'][year] = {}
            if year_latest_updated:
                continue
            elif 'E' in year:
                year_latest_updated = True
                year_latest = year_list[idx - 1]
                is_year = True

        if is_year:
            dict_input[type + '_latest'] = int(year_latest)
        else:
            dict_input[type + '_latest'] = -1

        tr_list = table_item.select('tbody > tr')  # 표의 내용 부분
        th_keylist = []
        for tr_idx, tr_item in enumerate(tr_list):
            th_list = tr_item.select('th')  # key값들
            th_keylist.append(self._translator(removeEmpty(th_list[0].text)))
            td_list = tr_item.select('td')  # value값들
            for td_idx in range(len(td_list)):
                dict_input['financial_info'][year_list[td_idx]
                                             ][th_keylist[-1]] = self._getValueByUnitType(td_list[td_idx].text, self.financial_info_value_type[tr_idx])
        return True, dict_input
    '''

    def _updateFinancial(self, dict_input, type="year"):
        # if 'year_latest' in dict_input.keys():
        #     if dict_input['year_latest']+1 >= datetime.date.today().year:
        #         return False, dict_input
        if type == "year":
            result, page = self._getPageSource()
        elif type == "quarter":
            result, page = self._getPageSource("quarter")

        if result == False:
            return False, dict_input

        soup = BeautifulSoup(page, 'html.parser')
        table_list = soup.select('table.gHead01')
        table_item = None
        for table in table_list:
            caption = removeEmpty(table.select('caption.blind')[0].text)
            if "주요재무정보" == caption:
                table_item = table
                break
        if table_item is None:
            return False, dict_input

        jongmok_name = soup.select('span.name')[0].text
        corp_WICS = removeEmpty(soup.select('td.td0101')[0].select(
            'dt.line-left')[2].text.split(':')[1])
        dict_input['jongmok_code'] = self.jongmok_code
        dict_input['jongmok_name'] = jongmok_name
        dict_input['corp_WICS'] = corp_WICS
        if 'financial_info' not in dict_input.keys():
            dict_input['financial_info'] = {}

        tr_list = table_item.select('thead > tr')  # 표의 제목 부분
        year_list = []
        for tr_idx, tr_item in enumerate(tr_list):
            th_list = tr_item.select('th')
            if tr_idx == 0:
                continue
            else:
                for th_idx in range(len(th_list)):
                    year_list.append(self._yearMonthMaker(
                        removeEmpty(th_list[th_idx].text), type))

        year_latest_updated = False
        is_year = False
        for idx, year in enumerate(year_list):
            if year not in dict_input['financial_info'].keys() and len(year) != 0:
                dict_input['financial_info'][year] = {}
            if year_latest_updated:
                continue
            elif 'E' in year:
                year_latest_updated = True
                year_latest = year_list[idx - 1]
                is_year = True

        if is_year:
            dict_input[type + '_latest'] = int(year_latest)
        else:
            dict_input[type + '_latest'] = -1

        tr_list = table_item.select('tbody > tr')  # 표의 내용 부분
        th_keylist = []
        for tr_idx, tr_item in enumerate(tr_list):
            th_list = tr_item.select('th')  # key값들
            th_keylist.append(self._translator(removeEmpty(th_list[0].text)))
            td_list = tr_item.select('td')  # value값들
            for td_idx in range(len(td_list)):
                if len(year_list[td_idx]) != 0:
                    dict_input['financial_info'][year_list[td_idx]
                                                 ][th_keylist[-1]] = self._getValueByUnitType(td_list[td_idx].text, self.financial_info_value_type[tr_idx])
        return True, dict_input

    def _int(self, text):
        try:
            return int(''.join(text.split(',')))
        except:
            self._writeFinancialLog(
                err_type="INVALID_VAL_INT", err_detail={"value": text})
            self.errlog("cannot convert to integer.(TEXT: {})".format(text))
            return -1

    def _float(self, text):
        try:
            return float(''.join(text.split(',')))
        except:
            self._writeFinancialLog(
                err_type="INVALID_VAL_FLOAT", err_detail={"value": text})
            self.errlog("cannot convert to float.(TEXT: {})".format(text))
            return -1.0

    def _getValueByUnitType(self, text, unit_type):
        try:
            if unit_type == '억원':
                new_text = text.split("조")
                regex = re.compile("[0-9,]+")
                if len(new_text) > 1:
                    cho = self._int(regex.search(new_text[0]).group())
                    uk = self._int(regex.search(new_text[1]).group())
                else:
                    cho = 0
                    uk = self._int(regex.search(new_text[0]).group())
                return (cho*10000 + uk) * 100000000
            elif unit_type == 'int':
                regex = re.compile("[0-9,.-]+")
                return self._int(regex.search(text).group())
            elif unit_type == 'float':
                regex = re.compile("[0-9,.-]+")
                return self._float(regex.search(text).group())
            elif unit_type == 'rate':
                regex = re.compile("[0-9,.-]+")
                return self._float(regex.search(text).group())/100
            elif unit_type == 'signed':
                regex = re.compile("[0-9,.]+")
                val = self._float(regex.search(text).group())
                if ("매수" in text) or ("+" in text):
                    return val
                else:
                    return -1 * val
            elif unit_type == 'rate signed':
                regex = re.compile("[0-9,.]+")
                val = self._float(regex.search(text).group())
                if ("매수" in text) or ("+" in text):
                    return val/100
                elif ("매도" in text) or ("-" in text):
                    return -1 * val/100
                return val/100
            else:
                self._writeFinancialLog(
                    err_type="INVALID_VAL", err_detail={"value": text, "unit_type": unit_type})
                self.errlog("wrong unit type.(TYPE: {})".format(unit_type))
                return -1
        except:
            self._writeFinancialLog(
                err_type="INVALID_VAL", err_detail={"value": text, "unit_type": unit_type})
            return -1

    def _updateInvestFinancial(self, dict_input):
        # if 'year_latest' in dict_input.keys():
        #     if dict_input['year_latest']+1 >= datetime.date.today().year:
        #         return False, dict_input
        result, page = self._getPageSource('financial')
        if result == False:
            return False, dict_input

        soup = BeautifulSoup(page, 'html.parser')
        table_list = soup.select('div#tab_con1')
        if len(table_list) == 0:
            return False, dict_input

        th_list = table_list[0].select('th')
        td_list = table_list[0].select('td')
        th_text = [th.text for th in th_list]
        td_text = [td.text for td in td_list]

        key_index = 0
        key_list = []
        for th in th_text:
            sub_list = th.split("l")
            for atom in sub_list:
                if self.invest_info_keys_kor[key_index] == '':
                    if self.invest_info_keys_kor[key_index + 1] in atom:
                        key_index += 1
                        key_list.append(self.invest_info_keys_eng[key_index])
                elif self.invest_info_keys_kor[key_index] in atom:
                    key_list.append(self.invest_info_keys_eng[key_index])
                else:
                    key_list.append('')
                    self._writeFinancialLog(
                        err_type="MISSING_INFO", err_detail={"key": self.invest_info_keys_eng[key_index]})
                    self.errlog("missing investment info.(CODE: {})(KEY: {})".format(
                        self.jongmok_code, self.invest_info_keys_eng[key_index]))
                key_index += 1

        val_index = 0
        val_list = []
        for td in td_text:
            sub_list = td.split("l")
            for atom in sub_list:
                val = self._getValueByUnitType(
                    atom, self.invest_info_value_type[val_index])
                if val == -1:
                    self._writeFinancialLog(
                        err_type="MISSING_VAL", err_detail={"key": key_list[val_index]})
                    self.errlog("missing investment info value.(CODE: {})(KEY: {})".format(
                        self.jongmok_code, key_list[val_index]))
                val_list.append(val)
                val_index += 1

        dict = {}
        for idx, key in enumerate(key_list):
            dict[key] = val_list[idx]
        dict_input['invest_info'] = dict

        return True, dict_input

    def crawl(self):
        save_name = f'{self.save_dir}/{self.jongmok_code}.json'
        if os.path.isfile(save_name):
            with open(save_name, "r", encoding='utf-8') as json_file:
                dict_input = json.load(json_file)
            self.log(
                "found previous financial data.(CODE: {})".format(self.jongmok_code))
            val0 = True
        else:
            dict_input = {}
            val0 = False
        val, dict = self._updateFinancial(dict_input, "year")
        _, dict = self._updateFinancial(dict, "quarter")
        val2, dict = self._updateInvestFinancial(dict)
        if val or val2:
            with open(save_name, "w", encoding='utf-8') as json_file:
                json.dump(dict, json_file, indent=4, ensure_ascii=False)
            with open(self.save_code_list_path, 'r', encoding='utf-8') as json_file:
                dict_list = json.load(json_file)
            if self.jongmok_code not in dict_list.keys():
                dict_list[self.jongmok_code] = []
            dict_list[self.jongmok_code].append(
                datetime.date.today().strftime("%Y/%m/%d"))
            with open(self.save_code_list_path, 'w', encoding='utf-8') as json_file:
                json.dump(dict_list, json_file, indent=4, ensure_ascii=False)
            self.log('saved financial data.(CODE: {})'.format(self.jongmok_code))
            return True
        else:
            self._writeFinancialLog(
                err_type="MISSING_DATA")
            self.errlog(
                'error occured while getting new financial data.(CODE: {})'.format(self.jongmok_code))
            return val0

    def get(self):
        save_name = f'{self.save_dir}/{self.jongmok_code}.json'
        if os.path.isfile(save_name):
            with open(save_name, "r", encoding='utf-8') as json_file:
                json_data = json.load(json_file)
            self.log('getting saved financial data.(CODE: {})'.format(
                self.jongmok_code))
            return json_data
        else:
            self._writeFinancialLog(
                err_type="MISSING_DATA_SAVED")
            self.errlog(
                'there is no saved financial data.(CODE: {})'.format(self.jongmok_code))
            return -1

    def _quarterFilter(self, data):
        quarter_latest = data['quarter_latest']
        quarter_data = data['financial_info'][str(quarter_latest)]
        op_income = quarter_data['op_income']
        debt_r = quarter_data['debt_r']
        PER = quarter_data['PER']
        self.log("op_income: {}; debt_rate: {}; PER: {}".format(
            op_income, debt_r, PER))
        return (op_income > 0) and (debt_r < 300) and (PER > 0)

    def _corpFilter(self, data):
        corp = data['corp_WICS']
        self.log("corp_WICS: {}".format(corp))
        return data['corp_WICS'] in self.corp_filter_list

    def filter(self, jongmok_code, prev_echo=False):
        self.jongmok_code = jongmok_code
        self.echo = prev_echo
        self.crawl()
        data = self.get()
        if data == -1:
            self._writeFinancialLog(state=-1, err_type="NORMAL")
            return {"state": -1, "data": {}}  # 매수 거부
        # self.echo = True
        self.log("investigate financial data.(CODE: {})".format(self.jongmok_code))
        val = self._quarterFilter(data)
        val2 = self._corpFilter(data)
        callback_data = {"corp_WICS": data["corp_WICS"], **
                         data['financial_info'][str(data['quarter_latest'])]}
        if val and val2:
            self._writeFinancialLog(state=1, err_type="NORMAL")
            self.log("investable jongmok code.")
            return {"state": 1, "data": callback_data}  # 매수 및 기록
        else:
            self._writeFinancialLog(state=-1, err_type="NORMAL")
            self.log("uninvestable jongmok code.")
            return {"state": -1, "data": {}}  # 매수 거부


if __name__ == "__main__":
    financial_agent = FinancialAgent(echo=False, echo_error=False)
    print(financial_agent.filter('000020', prev_echo=False))
    print(financial_agent.filter('089860', prev_echo=False))
    financial_agent.filter('038000', prev_echo=False)
    financial_agent.filter('038110', prev_echo=False)
    financial_agent.filter('000020', prev_echo=False)
    financial_agent.filter('093230', prev_echo=False)
