import sqlite3
import os
import pandas as pd
import numpy as np
import pickle
import time

class duplicateChecker:
    def __init__(self):
        self.database = {} # 코드를 키로 갖고 매각일자를 값으로 가짐
    
    def check(self, code, gongsi_date, sell_date): # 값 업데이트 후 중복여부 출력
        if code in self.database.keys():
            if not self._isBefore(self.database[code], gongsi_date):
                return True
        self.database[code] = sell_date
        return False           
        
    def _isBefore(self, A, B): # A <= B 체커
        (yA, mA, dA) = [int(a) for a in A.split('-')]
        (yB, mB, dB) = [int(b) for b in B.split('-')]
        if yA < yB:
            return True
        elif yA == yB:
            if mA < mB:
                return True
            elif mA == mB and dA <= dB:
                return True
        return False

#가격 data load (pickle data)
#작업 경로 설정. '\\'대신 '/'로 수정

abs_path = os.getcwd()
cur_path = abs_path
abs_path = abs_path.replace('\\','/')
abs_path = '/'.join(abs_path.split('/')[:-1])

#data 긁어오기
target_data_list = []
total_data = os.listdir('{}/kw/prices/all_daily_data/'.format(abs_path))
for target_name in total_data:
    target_data_list.append('{}/kw/prices/all_daily_data/{}'.format(abs_path, target_name))
    
target_data_list_modified = []
total_data_modified = os.listdir('{}/kw/prices/all_daily_data_modified_close/'.format(abs_path))
for target_name_modified in total_data_modified:
    target_data_list_modified.append('{}/kw/prices/all_daily_data_modified_close/{}'.format(abs_path, target_name_modified))
        
#DB 로드 후 Pandas로 연결
conn = sqlite3.connect('./project.db')
cur = conn.cursor()
cur.execute("SELECT * FROM post")
rows = cur.fetchall()
data_lengh = len(rows)
cols = [column[0] for column in cur.description]
data_df = pd.DataFrame.from_records(data=rows, columns=cols)


#Test 변수들 세팅
#buy_waiting_period = 5 #공시 후 매수까지 avg_buy_price < target_date_low_price 되는지 기다리는 기간
#holding_period = 60 #매수 후 매도까지 홀딩하는 기간. 이 기간동안 목표 수익률을 넘게 될 경우 기간 전이라도 매도
#selling_plus_percent = 0.10 #홀딩기간 동안 익절 퍼센트
#selling_minus_percent = 0.05 #홀딩기간 동안 손절 퍼센트

import traceback


#로드한 DB data 분석
def run(buy_waiting_period, holding_period, selling_plus_percent,selling_minus_percent):
    dup = duplicateChecker()
    try:
        #저장 변수들 세팅
        target_data_dict = dict() #post된 데이터를 불러와 target_data로 저장하여 조건 분석
        total_trading_data = []
        
        #for i in range(30000,data_lengh):
        for i in range(data_lengh):
            filtered = False #필터되는 data확인
            buyprice_low_check = False
            
            
            trading_data = [] #사고 파는 종목 데이터 리스트 세팅

            target_data_dict['date'] = data_df.loc[i]['date']
            target_data_dict['corp_name'] = data_df.loc[i]['corp_name']    
            target_data_dict['corp_code'] = data_df.loc[i]['corp_code']
            target_data_dict['corp_market'] = data_df.loc[i]['corp_market']
            target_data_dict['title'] = data_df.loc[i]['title']
            target_data_dict['url'] = data_df.loc[i]['url']
            target_data_dict['buy_sell'] = data_df.loc[i]['buy_sell']
            target_data_dict['avg_buy_price'] = data_df.loc[i]['avg_buy_price']                    
            
            ###################여기 코드 작업##########################
            
            #0차 필터링 : konex인 경우 제외
            if target_data_dict['corp_market'] == "konex":
                filtered = True
                continue            

            #1차 필터링 : buy인 경우만 필터
            if target_data_dict['buy_sell'] == "sell":
                filtered = True
                continue
            else:
                if float(target_data_dict['avg_buy_price']) == 0:#2차 필터링 : avg_buy_price가 0원일 경우 제외
                    #print("avg_buy_price가 0원이여서 제외")
                    filtered = True
                    continue

            for target_data_name in target_data_list: #3차 필터링 : avg_buy_price < 종목의 다음 날 시가 ~ 5영업일 이후
                target_code = target_data_name.split('/')[-1].split('.')[0]
                if target_data_dict['corp_code'] == target_code:
                    #print("종목 찾음!", target_code)
                
                    with open(target_data_name, 'rb') as f: #찾은 종목 pickle data load
                        target_data = pickle.load(f)

                    target_date_list = target_data.index #날짜 load
                    target_idx = None
                    target_date = None
                    for target_date_fake in target_date_list[300:-2]:
                        if target_data_dict['date'] == target_date_fake:
                            target_idx = list(target_data.index).index(target_date_fake)                    
                            #print("post된 avg_buy_price : ", target_data_dict['avg_buy_price'])
                            target_date = target_date_fake
                            break

                    target_date_plus = 0 # 매수 기다리는 날짜 카운트 변수 
                    target_date_day_open = dict()
                    target_date_day_low = dict()

                    if target_idx == None:
                        break

                    while target_date_plus < buy_waiting_period: 
                        target_date_plus += 1

                        target_date_day_open[target_date_plus] = target_data.iloc[target_idx + target_date_plus]['open']
                        target_date_day_low[target_date_plus] = target_data.iloc[target_idx + target_date_plus]['low']
                        if target_date_day_open[target_date_plus] > float(target_data_dict['avg_buy_price']):
                            #print(target_date,"일 + ",target_date_plus, "의 시가",target_date_day_open[target_date_plus])
                            #print("평균 매수 단가가", target_date,"일 + ",target_date_plus, "일 시가보다 낮음!")

                            if target_date_day_low[target_date_plus] <= float(target_data_dict['avg_buy_price']):
                                print(target_date,"일 + ",target_date_plus, "의 시가/저가",target_date_day_open[target_date_plus],"/", target_date_day_low[target_date_plus])
                                print("평균 매수 단가가", target_date,"일 + ",target_date_plus, "일 시가보다 낮고, 저가보다는 높음!")
                                print("매수!!!!")

                                buyprice_low_check = True  # 저가 < avgbuy_pirce < 시가 일 경우 구분 
                                break

                            else:
                                continue

                        else:
                            print(target_date,"일 + ",target_date_plus, "의 시가",target_date_day_open[target_date_plus])
                            print("평균 매수 단가가", target_date,"일 + ",target_date_plus, "일 시가보다 높음!")
                            print("매수!!!!")
                            break

                    if target_date_plus == buy_waiting_period: # buy_waiting 기간 도래 후 while문 종료 시. 다음 종목으로
                        continue


                    #buy_target_date_list = []
                    buy_total_cash = 10000000 # 각 종목 당 매수 금액

                    #수정 종가 반영
                    target_data_modified = None
                    for target_data_name_modified in target_data_list_modified:
                        target_code = target_data_name_modified.split('/')[-1].split('.')[0]
                        if target_data_dict['corp_code'] == target_code:
                            #print("수정 종가 파일에서 종목 찾음!", target_code)

                            with open(target_data_name_modified, 'rb') as f: #찾은 종목 pickle data load
                                target_data_modified = pickle.load(f)
                            break

                    target_date_modified_list = target_data_modified.index #날짜 load
                    for target_date_modified in target_date_modified_list[:-2]:
                        if target_data_dict['date'] == target_date_modified:

                            #오류 확인
                            if target_date_modified != target_date:
                                print("시발 뭐지")
                                assert false, 'fuck'
                                
                            target_idx_modified = list(target_data_modified.index).index(target_date_modified)
                            break
                    #매수 가격 수정 종가로 전환

                    if buyprice_low_check == True:
                        if len(target_data_modified) < (target_idx_modified + target_date_plus):
                            print("찾았다 도형 pickle에 있는 데이터보다 target_date_plus를 합친 idx가 넘어감!! 예외처리")
                            break
                        modified_factor = float(target_data_modified.iloc[target_idx_modified + target_date_plus]['low'] / target_data.iloc[target_idx + target_date_plus]['low'])
                        buyprice_modified = round(float(target_data_dict['avg_buy_price']) * modified_factor, 3)
                        #print("low_check == True!! buyprice_modified = ", buyprice_modified)
                        #print("참고용!! target_data_modified_low = ", target_data_modified.iloc[target_idx_modified + target_date_plus]['low'])


                    else:
                        if len(target_data_modified) < (target_idx_modified + target_date_plus):
                            print("찾았다 도형. buyprice_low_check = False. pickle에 있는 데이터보다 target_date_plus를 합친 idx가 넘어감!! 예외처리")
                            break
                        #print("김도형ㅄ6")
                        buyprice_modified = target_data_modified.iloc[target_idx_modified + target_date_plus]['open']
                        print("low_check == False!! buyprice_modified = ", buyprice_modified)

                    #list에 저장

                    #list(target_data_modified.index)[target_idx_modified + 1]
                    buy_qty = float(buy_total_cash / buyprice_modified)
                    buy_date = list(target_data_modified.index)[target_idx_modified + target_date_plus]
                    print("기준날짜 : ",target_date, "매수 날짜 : ", buy_date)
                    trading_data.append(target_date) 
                    trading_data.append(target_data_dict['url'])
                    trading_data.append(target_data_dict['corp_name']) #종목명 저장
                    trading_data.append(target_code) #매수 대상 종목 코드 저장
                    trading_data.append(target_data_dict['corp_market'])
                    trading_data.append(buy_date) #매수 날짜 저장
                    trading_data.append(buyprice_modified) #수정 종가 반영 후 매수가격
                    trading_data.append(buy_total_cash)

                    #buy_data.append(buy_target_date_list)

                    #매도 반영 및 수익률 분석
                    holding_date_plus = 0 # 홀딩 날짜 카운트 변수 
                    holding_date_day_high = dict() # 홀딩 기간 동안의 고가 저장 dict / for 익절 분석
                    holding_date_day_low = dict() # 홀딩 기간 동안의 저가 저장 dict / for 손절 분석
                    #selling_target_date_list = []
                    while holding_date_plus < holding_period:
                        holding_date_plus += 1

                        # 홀딩기간 익절 매도 금액 분석
                        
                        if len(target_data_modified) < (target_idx_modified + holding_date_plus):
                            print("찾았다 도형. '익절부분!!' pickle에 있는 데이터보다 holding_date_plus를 합친 idx가 넘어감!! 예외처리")
                            break
                        
                        holding_date_day_high[holding_date_plus] = target_data_modified.iloc[target_idx_modified + holding_date_plus]['high']
                        
                        sellingprice_modified = round(buyprice_modified * (1+ selling_plus_percent), 3)
                        if float(holding_date_day_high[holding_date_plus]) >= sellingprice_modified:
                            print(holding_date_plus, "날짜의 익절 발생!!")

                            selling_total_cash = sellingprice_modified * buy_qty
                            selling_date = list(target_data_modified.index)[target_idx_modified + holding_date_plus]
                            print("기준날짜 : ",target_date, "매도 날짜 : ", selling_date)
                            trading_data.append(selling_date) # 매수 날짜 저장 / date_plus이후 수정 필요
                            trading_data.append(sellingprice_modified) #수정 종가 반영 매도 금액
                            trading_data.append(selling_total_cash) # 총 매도 익절 금액
                            return_percent = round((selling_total_cash - buy_total_cash) / buy_total_cash, 5)
                            trading_data.append(return_percent) # 수익률 추가
                            trading_data.append("익절")

                            #sell_data.append(selling_target_date_list)

                            print(trading_data)
                            
                            break

                        # 홀딩기간 손절매도 금액 분석
                        
                        if len(target_data_modified) < (target_idx_modified + holding_date_plus):
                            print("찾았다 도형. '손절부분!!' pickle에 있는 데이터보다 holding_date_plus를 합친 idx가 넘어감!! 예외처리")
                            break
                        
                        holding_date_day_low[holding_date_plus] = target_data_modified.iloc[target_idx_modified + holding_date_plus]['low']
                        sellingprice_modified = round(buyprice_modified * (1 - selling_minus_percent), 3)
                        if float(holding_date_day_low[holding_date_plus]) <= sellingprice_modified:
                            print("손절 발생!!!!!!!!!!!!!!!!!")

                            selling_total_cash = sellingprice_modified * buy_qty
                            selling_date = list(target_data_modified.index)[target_idx_modified + holding_date_plus]
                            print("기준날짜 : ",target_date, "매도 날짜 : ", selling_date)
                            trading_data.append(selling_date) # 매수 날짜 저장 / date_plus이후 수정 필요
                            trading_data.append(sellingprice_modified) #수정 종가 반영 매도 금액
                            trading_data.append(selling_total_cash) # 총 매도 손절 금액
                            return_percent = round((selling_total_cash - buy_total_cash) / buy_total_cash, 5)
                            trading_data.append(return_percent) # 수익률 추가
                            trading_data.append("손절")


                            print("저가 가격 : ", holding_date_day_low[holding_date_plus])
                            print("손절 대상 가격 : ", sellingprice_modified)

                            print(trading_data)
                            
                            break

                        if holding_date_plus == holding_period:
                            #print("지정 i 횟수 ", i)
                            print(holding_period, "기간 도래! 종가에 매도")

                            if len(target_data_modified) < (target_idx_modified + holding_date_plus):
                                print("찾았다 도형. '기간도래부분!!' pickle에 있는 데이터보다 holding_date_plus를 합친 idx가 넘어감!! 예외처리")
                                break

                            sellingprice_modified = target_data_modified.iloc[target_idx_modified + holding_date_plus]['close']
                            selling_total_cash = float(sellingprice_modified) * buy_qty
                            selling_date = list(target_data_modified.index)[target_idx_modified + holding_date_plus]
                            print("기준날짜 : ",target_date, "매도 날짜 : ", selling_date)
                            trading_data.append(selling_date)
                            trading_data.append(sellingprice_modified) #수정 종가 반영 매도 금액
                            trading_data.append(selling_total_cash) # 총 매도 익절 금액
                            return_percent = round((selling_total_cash - buy_total_cash) / buy_total_cash, 5)
                            trading_data.append(return_percent) # 수익률 추가
                            trading_data.append("홀딩기간 도래하여 종가 매도")

                            print(trading_data)
                            
                            break
                        
                        
                    
                    if dup.check(trading_data[3], trading_data[0], trading_data[8]) == True:
                        print("중복되었다!!!!")
                        print(trading_data[3], trading_data[0], trading_data[8], "종목코드, 공시날짜, selling 날짜")
                        filtered = True
                        continue
                    
                    ##이중리스트, 최종 리스트에 추가
                    if filtered == False:
                        total_trading_data.append(trading_data)
                              
                    else:
                        print("filtered에서 오류발견!!!!")
                        assert false, 'fuck'

        print("###############################################################")
        print("###############################################################")
        print("최종 trading data")
        print(total_trading_data)

        #print(target_data_list)
        conn.close()
        #print(data_df)
    except Exception as e:
        print(traceback.format_exc())
    
    return total_trading_data