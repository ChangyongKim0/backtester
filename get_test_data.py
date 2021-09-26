from utils import *

from PyQt5.QAxContainer import *
from PyQt5.QtWidgets import *
from PyQt5.QtCore import *
from PyQt5.QtGui import *

from datetime import datetime, timedelta
from copy import deepcopy
import pandas as pd
import numpy as np
import pickle
import random
import time
import glob
import sys
import os

import matplotlib.ticker as ticker
import matplotlib.pyplot as plt
import mpl_finance
from matplotlib import font_manager, rc

font_name = font_manager.FontProperties(fname="c:/Windows/Fonts/malgun.ttf").get_name()
rc('font', family=font_name)

abs_path = os.path.realpath(__file__).split('\\')[:-2]
abs_path = '/'.join(abs_path)

TR_REQ_TIME_INTERVAL = 1.0 #0.2
SCREEN_NO = '1000'

class Agent:
    def __init__(self, window):
        self.today_date = datetime.strftime(datetime.now(),"%Y%m%d")
        self.window = window
        self.code_list = []
        self.save_list = []
        self.data_num = None
        self.day0_date = None
        if os.path.exists('day_before.pkl'):
            with open('day_before.pkl', 'rb') as f:
                self.day_before, is_complete = pickle.load(f)
            if is_complete:
                self.day_before -= 1
        else:
            self.day_before = 23
        assert self.day_before >= 2

    def event_connect(self, kiwoom):
        #################### 조건검색하기 #######################
        kiwoom.condition_name = '5선이격조건{}일봉전'.format(self.day_before)
        kiwoom.get_condition_load()
        loop = QEventLoop()
        kiwoom.signal.finished.connect(loop.quit)
        loop.exec_()
        self.code_list = deepcopy(kiwoom.cond_code_list)

        #################### 일봉데이터 가져오기 #######################
        self.daily_data_list = []
        for code in self.code_list:
            kiwoom.get_daily_price_data(code, self.today_date)
            loop = QEventLoop()
            kiwoom.signal.finished.connect(loop.quit)
            loop.exec_()

            daily_data = deepcopy(kiwoom.daily_data)
            if code in kiwoom.stockCodeList_KOSPI:
                market_type = 'KOSPI'
            elif code in kiwoom.stockCodeList_KOSDAQ:
                market_type = 'KOSDAQ'
            else:
                market_type = 'ETC'
            self.daily_data_list.append([code, daily_data, market_type])
        self.data_num = len(self.daily_data_list)
        print("조건식 만족 코드 가져오기 성공! 총 {}개".format(self.data_num))

        if self.data_num == 0:
            print("조건식 만족 코드 없음 종료")
            with open('day_before.pkl', 'wb') as f:
                pickle.dump([self.day_before, True], f)
            kiwoom.program_exit()
            return

        date_list = self.daily_data_list[0][1].index
        self.day0_date = date_list[-1-self.day_before]

        if self.day_before > 2:
            remove_rows = date_list[2-self.day_before:]
            for i, daily_data in enumerate(self.daily_data_list):
                self.daily_data_list[i][1].drop(remove_rows, axis=0, inplace=True)

        #################### 일봉데이터 유무확인 #######################
        if not os.path.isdir('{}/data/{}'.format(abs_path, self.day0_date)):
            os.mkdir('{}/data/{}'.format(abs_path, self.day0_date))
        with open('{}/data/{}/daily_data.pkl'.format(abs_path, self.day0_date), 'wb') as f:
            pickle.dump(self.daily_data_list, f)

        #time.sleep(60 + random.random()*2)

        #####################################아래부터는 분봉데이터###################################

        minute_data_dir = '{}/data/minute_data'.format(abs_path)
        if not os.path.isdir(minute_data_dir):
            os.mkdir(minute_data_dir)

        for idx in range(len(self.daily_data_list)):
            jongmok_code, daily_data, market_type = self.daily_data_list[idx]
            original_data = []
            if '{}.pkl'.format(jongmok_code) in os.listdir('{}/'.format(minute_data_dir)):
                print('[{}] 분봉데이터 있음'.format(jongmok_code))
                with open('{}/{}.pkl'.format(minute_data_dir, jongmok_code), 'rb') as f:
                    original_data = pickle.load(f)
                #continue
            print('[{}] 분봉데이터 긁어오기 시작!'.format(jongmok_code))
            kiwoom.min_data = {'open':[], 'high':[], 'low':[], 'close':[], 'volume':[]}
            kiwoom.min_date = []

            next = 0
            while True:
                if kiwoom.cnt >= 999:
                    print("과도한 데이터 요청 방지 프로그램 종료")
                    with open('day_before.pkl', 'wb') as f:
                        pickle.dump([self.day_before, False], f)
                    kiwoom.program_exit()
                    return

                loop = QEventLoop()
                kiwoom.signal.finished.connect(loop.quit)
                kiwoom.get_min_price_data(jongmok_code, next)
                loop.exec_()
                next = kiwoom.miniute_next
                if next == 0:
                    break
                if len(original_data) != 0:
                    if kiwoom.min_date[-1] in original_data.index:
                        break

            kiwoom.min_date = kiwoom.min_date[::-1]
            for col in ['open','high','low','close','volume']:
                kiwoom.min_data[col] = kiwoom.min_data[col][::-1]
            data = pd.DataFrame(kiwoom.min_data, columns=['open', 'high', 'low', 'close', 'volume'], index=kiwoom.min_date)

            if len(original_data) != 0:
                data = data.loc[original_data.index[-1]:]
                if len(data) > 1:
                    data = data.loc[data.index[1]:]
                    data = original_data.append(data)
                else:
                    data = original_data
            
            with open('{}/{}.pkl'.format(minute_data_dir, jongmok_code), 'wb') as f:
                pickle.dump(data, f)
            print(datetime.strptime(kiwoom.min_date[0],'%Y%m%d%H%M%S'), "분봉데이터 조회 끝!!!!")
            #time.sleep(60 + random.random()*2)

        print("test data 긁어오기 종료")
        with open('day_before.pkl', 'wb') as f:
            pickle.dump([self.day_before, True], f)
        kiwoom.program_exit()
        return

#######################################################################################################

class MyWindow(QWidget):
    def __init__(self, app):
        super().__init__()
        self.app = app
        self.setupUI()
        self.agent = Agent(self)
        self.kiwoom = Kiwoom(self)

    def event_connect(self):
        self.agent.event_connect(self.kiwoom)

    def setupUI(self):
        self.setGeometry(100, 100, 100, 100)
        self.setWindowTitle("dobro")
        layout = QHBoxLayout()
        self.setLayout(layout)
        

class Custom_Signal(QWidget):
    #시그널 만들기
    finished = pyqtSignal(int)
    total_finished = pyqtSignal(int)
    def __init__(self):
        super().__init__()


class Kiwoom(QAxWidget):
    def __init__(self, window):
        super().__init__("KHOPENAPI.KHOpenAPICtrl.1")

        self.window = window
        self.signal = Custom_Signal()
        self.condition_name = None
        self.cnt = 0

        self.OnEventConnect.connect(self._event_connect)
        self.OnReceiveTrData.connect(self._receive_tr_data)
        self.OnReceiveRealData.connect(self._receive_real_data)
        self.OnReceiveMsg.connect(self._receive_msg)
        self.OnReceiveChejanData.connect(self._receive_chejan_data)
        self.OnReceiveConditionVer.connect(self._receive_condition_ver)
        self.OnReceiveTrCondition.connect(self._receive_tr_condition)

        self.signal.total_finished.connect(self.window.app.quit)
        
        self.comm_connect()

    def program_exit(self):
        print("프로그램 종료!")
        self.signal.total_finished.emit(0)

    def comm_connect(self):
        self.dynamicCall("CommConnect()")

    def set_input_value(self, id, value):
        self.dynamicCall("SetInputValue(QString, QString)", id, value)

    def comm_rq_data(self, rqname, trcode, next, screen_no):
        err = self.dynamicCall("CommRqData(QString, QString, int, QString)", rqname, trcode, next, screen_no)
        if err != 0:
            raise ValueError("[{} 요청]에서 에러 발생! 에러코드 : [{}]".format(rqname, err))
        self.cnt += 1
        print(self.cnt)
        time.sleep(TR_REQ_TIME_INTERVAL)

    def _get_comm_data(self, code, request_name, index, item_name):
        ret = self.dynamicCall("GetCommData(QString, QString, int, QString)", code, request_name, index, item_name)
        return ret.strip()

    def _get_comm_real_data(self, code, index):
        ret = self.dynamicCall("GetCommRealData(QString, int)", code, index)
        return ret.strip()

    def _get_chejan_data(self, fid):
        ret = self.dynamicCall("GetChejanData(int)", fid)
        return ret.strip()

    def get_daily_price_data(self, jongmok_code, end_day="20170224"):
        rqname = "일봉차트조회"
        trcode = "opt10081"
        screen_no = SCREEN_NO
        self.set_input_value("종목코드", jongmok_code)
        self.set_input_value("기준일자", end_day)
        self.set_input_value("수정주가구분", 1)
        self.comm_rq_data(rqname, trcode, 0, screen_no)
        print('[{} 요청] 종목코드 : {}'.format(rqname, jongmok_code))

    def get_min_price_data(self, jongmok_code, next=0):
        rqname = "분봉차트조회"
        trcode = "opt10080"
        screen_no = SCREEN_NO
        self.set_input_value("종목코드", jongmok_code)
        self.set_input_value("틱범위", 1)
        self.set_input_value("수정주가구분", 1)
        self.comm_rq_data(rqname, trcode, next, screen_no)
        #print('[{} 요청] 종목코드 : {}'.format(rqname, jongmok_code))

    def _event_connect(self, err_code):
        if err_code == 0:
            print("connected")

            self.stockCodeList_KOSPI = self.dynamicCall("GetCodeListByMarket(QString)", "0")
            self.stockCodeList_KOSPI = self.stockCodeList_KOSPI.split(';')[:-1]
            self.stockCodeList_KOSDAQ = self.dynamicCall("GetCodeListByMarket(QString)", "10")
            self.stockCodeList_KOSDAQ = self.stockCodeList_KOSDAQ.split(';')[:-1]
            account_list = self.dynamicCall("GetLoginInfo(QString)","ACCLIST")
            account_list = account_list.split(';')[:-1]
            
            self.window.event_connect()

        else:
            print("disconnected")

    def _receive_tr_data(self, screen_no, rqname, trcode, record_name, next, unused1, unused2, unused3, unused4):
        if rqname == "일봉차트조회":
            cnt = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
            jongmok_code = self._get_comm_data(trcode, rqname, 0, "종목코드")
            data = {'open':[], 'high':[], 'low':[], 'close':[], 'volume':[]}
            date = []
            for i in range(cnt-1, -1, -1):
                d = self._get_comm_data(trcode, rqname, i, "일자")
                date.append(d[:4]+'-'+d[4:6]+'-'+d[6:])
                data['open'].append(abs(int(self._get_comm_data(trcode, rqname, i, "시가"))))
                data['high'].append(abs(int(self._get_comm_data(trcode, rqname, i, "고가"))))
                data['low'].append(abs(int(self._get_comm_data(trcode, rqname, i, "저가"))))
                data['close'].append(abs(int(self._get_comm_data(trcode, rqname, i, "현재가"))))
                data['volume'].append(abs(int(self._get_comm_data(trcode, rqname, i, "거래량"))))
            data = pd.DataFrame(data, columns=['open', 'high', 'low', 'close', 'volume'], index=date)
            self.daily_data = data
            print('[{}] 종목코드 : {} 수신성공'.format(rqname,jongmok_code))
            self.signal.finished.emit(0)

        elif rqname == "분봉차트조회":
            cnt = self.dynamicCall("GetRepeatCnt(QString, QString)", trcode, rqname)
            jongmok_code = self._get_comm_data(trcode, rqname, 0, "종목코드")
            for i in range(cnt):
                self.min_date.append(self._get_comm_data(trcode, rqname, i, "체결시간"))
                self.min_data['open'].append(abs(int(self._get_comm_data(trcode, rqname, i, "시가"))))
                self.min_data['high'].append(abs(int(self._get_comm_data(trcode, rqname, i, "고가"))))
                self.min_data['low'].append(abs(int(self._get_comm_data(trcode, rqname, i, "저가"))))
                self.min_data['close'].append(abs(int(self._get_comm_data(trcode, rqname, i, "현재가"))))
                self.min_data['volume'].append(abs(int(self._get_comm_data(trcode, rqname, i, "거래량"))))
            self.miniute_next = int(next)
            self.signal.finished.emit(0)
            '''
            if next == "2":
                print(self.min_date[-1])
                self.get_min_price_data(jongmok_code, 2)
            else:
                self.min_date = self.min_date[::-1]
                for col in ['open','high','low','close','volume']:
                    self.min_data[col] = self.min_data[col][::-1]
                data = pd.DataFrame(self.min_data, columns=['open', 'high', 'low', 'close', 'volume'], index=self.min_date)
                with open(abs_path+'data/{}/minute_data_{}.pkl'.format(self.cond_date, jongmok_code), 'wb') as f:
                    pickle.dump(data, f)
                print(datetime.strptime(self.min_date[0],'%Y%m%d%H%M%S'), "분봉데이터 조회 끝!!!!")
                self.signal.finished.emit(0)
            '''


    def _receive_real_data(self, jongmok_code, real_type, real_data):
        pass

    def _receive_msg(self, screen_no, rqname, trcode, msg):
        #msg : [00z310] 모의투자 조회, [00z112] 매수 정상처리, [00z113] 매도 정상처리, [00z214] 매수 주문수량 오류
        print("[OnReceiveMsg] screen : {}, rqname : {}, trcode : {}, msg : {}".format(screen_no,rqname,trcode,msg))

    def _receive_chejan_data(self, gubun, item_cnt, fid_list):
        pass

    ##########################
    ######## 조건 검색 ########
    ##########################
    def get_condition_load(self):
        e = self.dynamicCall("GetConditionLoad()")
        if e == 0:
            raise ValueError("[조건검색 요청]에서 에러 발생!")
        time.sleep(TR_REQ_TIME_INTERVAL)

    def get_condition_name_list(self):
        value = self.dynamicCall("GetConditionNameList()")
        return value

    def send_condition(self, screen_no, cond_name, cond_idx, cond_type=0):
        #cond_type = 0:조건검색, 1:실시간 조건검색
        e = self.dynamicCall("SendCondition(QString, QString, int, int)", screen_no, cond_name, cond_idx, cond_type)
        if e == 0:
            raise ValueError("[조건종목 요청]에서 에러 발생!")
        time.sleep(TR_REQ_TIME_INTERVAL)

    def _receive_condition_ver(self, ret, msg):
        print("[조건검색] return : {}, msg : {}".format(ret, msg))
        value = self.get_condition_name_list().split(';')
        condition_list = []
        for item in value:
            condition = item.split('^')
            if len(condition) == 2 and condition[1] == self.condition_name:
                condition_list = condition
                break
        
        if len(condition_list) == 0:
            raise ValueError('[{}]을 만족하는 조건식이 없음!'.format(self.condition_name))
        
        print('[조건검색결과] {}'.format(condition_list))
        self.send_condition(SCREEN_NO, condition_list[1], condition_list[0])

    def _receive_tr_condition(self, screen_no, code_list, cond_name, idx, next):
        print("[조건종목] 데이터 도착 | screen : {}, cond_name : {}, idx : {}, next : {}".format(screen_no, cond_name, idx, next))
        code_list = code_list.split(';')
        if len(code_list) > 0:
            code_list = code_list[:-1]
        self.cond_code_list = code_list
        self.signal.finished.emit(0)

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = MyWindow(app)
    window.show()
    app.exec_()
