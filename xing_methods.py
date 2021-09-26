import win32com.client
import pythoncom
from xing_login import logIn
import event_logger as log
import time
import json
# 현재가 조회


class XAQueryEventHandlerT1102:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT1102.query_state = 1

# 종목리스트조회


class XAQueryEventHandlerT8430:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT8430.query_state = 1

# 종목가격데이터조회


class XAQueryEventHandlerT8413:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT8413.query_state = 1


class XAQueryEventHandlerT1305:
    query_state = 0

    def OnReceiveData(self, code):
        XAQueryEventHandlerT1305.query_state = 1


def getCurrentPrice(jongmok_code):
    instXAQueryT1102 = win32com.client.DispatchWithEvents(
        "XA_DataSet.XAQuery", XAQueryEventHandlerT1102)
    instXAQueryT1102.ResFileName = "C:\\eBEST\\xingAPI\\Res\\t1102.res"
    instXAQueryT1102.SetFieldData("t1102InBlock", "shcode", 0, jongmok_code)
    instXAQueryT1102.Request(0)

    while XAQueryEventHandlerT1102.query_state == 0:
        pythoncom.PumpWaitingMessages()

    name = instXAQueryT1102.GetFieldData("t1102OutBlock", "hname", 0)
    price = instXAQueryT1102.GetFieldData("t1102OutBlock", "price", 0)
    log.write('종목이름: ', name)
    log.write('현재가: ', price)

    XAQueryEventHandlerT1102.query_state = 0
    return price


def getJongmokName(jongmok_code):
    instXAQueryT1102 = win32com.client.DispatchWithEvents(
        "XA_DataSet.XAQuery", XAQueryEventHandlerT1102)
    instXAQueryT1102.ResFileName = "C:\\eBEST\\xingAPI\\Res\\t1102.res"
    instXAQueryT1102.SetFieldData("t1102InBlock", "shcode", 0, jongmok_code)
    instXAQueryT1102.Request(0)

    while XAQueryEventHandlerT1102.query_state == 0:
        pythoncom.PumpWaitingMessages()

    name = instXAQueryT1102.GetFieldData("t1102OutBlock", "hname", 0)
    # log.write('종목이름: ', name)
    return name


def getFullJongmokCodeList(log_cut=-1):
    instXAQueryT8430 = win32com.client.DispatchWithEvents(
        "XA_DataSet.XAQuery", XAQueryEventHandlerT8430)
    instXAQueryT8430.ResFileName = "C:\\eBEST\\xingAPI\\Res\\t8430.res"

    instXAQueryT8430.SetFieldData("t8430InBlock", "gubun", 0, 1)
    instXAQueryT8430.Request(0)

    while XAQueryEventHandlerT8430.query_state == 0:
        pythoncom.PumpWaitingMessages()

    count = instXAQueryT8430.GetBlockCount("t8430OutBlock")
    log.write('전체 종목 수:', count)
    log.write('outputType:', '[{i, hname, shcode, expcode, etfgubun}]')
    for i in range(count):
        hname = instXAQueryT8430.GetFieldData("t8430OutBlock", "hname", i)
        shcode = instXAQueryT8430.GetFieldData("t8430OutBlock", "shcode", i)
        expcode = instXAQueryT8430.GetFieldData("t8430OutBlock", "expcode", i)
        etfgubun = instXAQueryT8430.GetFieldData(
            "t8430OutBlock", "etfgubun", i)
        if log_cut == -1 or i < log_cut:
            log.write(i, hname, shcode, expcode, etfgubun)

    XAQueryEventHandlerT8430.query_state = 0


def getJongmokCodeList(log_cut=-1):
    instXAQueryT8430 = win32com.client.DispatchWithEvents(
        "XA_DataSet.XAQuery", XAQueryEventHandlerT8430)
    instXAQueryT8430.ResFileName = "C:\\eBEST\\xingAPI\\Res\\t8430.res"

    instXAQueryT8430.SetFieldData("t8430InBlock", "gubun", 0, 1)
    instXAQueryT8430.Request(0)

    while XAQueryEventHandlerT8430.query_state == 0:
        pythoncom.PumpWaitingMessages()

    count = instXAQueryT8430.GetBlockCount("t8430OutBlock")
    log.write('전체 종목 수:', count)
    log.write('outputType:', '{shcode:{idx, hname}}')

    array = []
    for i in range(count):
        hname = instXAQueryT8430.GetFieldData("t8430OutBlock", "hname", i)
        shcode = instXAQueryT8430.GetFieldData("t8430OutBlock", "shcode", i)
        array.append(shcode)
        if log_cut == -1 or i < log_cut:
            log.write(i, hname, shcode)

    XAQueryEventHandlerT8430.query_state = 0
    return array


def getFullPriceData(code, start_date, end_date, _to_append=[], _to_append_idx={}):
    to_append = [*_to_append]
    to_append_idx = {**_to_append_idx}

    instXAQueryT8413 = win32com.client.DispatchWithEvents(
        "XA_DataSet.XAQuery", XAQueryEventHandlerT8413)
    instXAQueryT8413.ResFileName = "C:\\eBEST\\xingAPI\\Res\\t8413.res"

    instXAQueryT8413.SetFieldData("t8413InBlock", "shcode", 0, code)
    instXAQueryT8413.SetFieldData("t8413InBlock", "gubun", 0, "2")
    instXAQueryT8413.SetFieldData("t8413InBlock", "sdate", 0, start_date)
    instXAQueryT8413.SetFieldData("t8413InBlock", "edate", 0, end_date)
    instXAQueryT8413.SetFieldData("t8413InBlock", "comp_yn", 0, "N")

    instXAQueryT8413.Request(0)

    while XAQueryEventHandlerT8413.query_state == 0:
        pythoncom.PumpWaitingMessages()

    count = instXAQueryT8413.GetBlockCount("t8413OutBlock1")

    log.write('Price data recieved: total {} dates'.format(count))

    if count == 1:
        return False, to_append, to_append_idx
    idx_start = len(to_append)
    for i in range(count):
        date = instXAQueryT8413.GetFieldData("t8413OutBlock1", 'date', i)
        to_append_idx[date] = idx_start+i
        to_append.append({'date': date})
        for price_type in ['open', 'close', 'high', 'low', 'jdiff_vol', 'value', 'jongchk', 'rate', 'pricechk', 'ratevalue', 'sign']:
            # price_type : open, high, low, close
            price_data = instXAQueryT8413.GetFieldData(
                "t8413OutBlock1", price_type, i)
            if price_type == 'rate':
                price_data = float(price_data)
            else:
                price_data = int(price_data)
            to_append[-1][price_type] = price_data
        # log.write(to_append[-1])

    XAQueryEventHandlerT8413.query_state = 0
    return True, to_append, to_append_idx


def getTotalPriceData(code, date, count, is_first=True, _to_append=[], _to_append_idx={}):
    time.sleep(3)

    to_append = [*_to_append]
    to_append_idx = {**_to_append_idx}

    instXAQueryT1305 = win32com.client.DispatchWithEvents(
        "XA_DataSet.XAQuery", XAQueryEventHandlerT8413)
    instXAQueryT1305.ResFileName = "C:\\eBEST\\xingAPI\\Res\\t1305.res"

    instXAQueryT1305.SetFieldData("t1305InBlock", "shcode", 0, code)
    instXAQueryT1305.SetFieldData("t1305InBlock", "dwmcode", 0, "1")
    instXAQueryT1305.SetFieldData("t1305InBlock", "date", 0, date)
    instXAQueryT1305.SetFieldData("t1305InBlock", "cnt", 0, count)

    instXAQueryT1305.Request(not is_first)

    while XAQueryEventHandlerT8413.query_state == 0:
        pythoncom.PumpWaitingMessages()

    new_count = instXAQueryT1305.GetBlockCount("t1305OutBlock1")

    log.write('Price data recieved: total {} dates'.format(new_count))

    if new_count < 1:
        return False, to_append, to_append_idx
    idx_start = len(to_append)
    for i in range(new_count):
        date = instXAQueryT1305.GetFieldData("t1305OutBlock1", 'date', i)
        print(i, date)
        to_append_idx[date] = idx_start+i
        to_append.append({'date': date})
        for price_type in ['marketcap', 'open', 'close', 'high', 'low', 'sign', 'change', 'diff', 'volume', 'diff_vol', 'chdegree', 'sojinrate', 'changerate', 'fpvolume', 'covolume', 'value', 'ppvolume', 'o_sign', 'o_change', 'o_diff', 'h_sign', 'h_change', 'h_diff', 'l_sign', 'l_change', 'l_diff']:
            # price_type : open, high, low, close
            price_data = instXAQueryT1305.GetFieldData(
                "t1305OutBlock1", price_type, i)
            # if price_type in ['open', 'high', 'low', 'close']:
            #     price_data = float(price_data)
            # else:
            #     price_data = int(price_data)
            to_append[-1][price_type] = price_data
        # log.write(to_append[-1])
    date_new = instXAQueryT1305.GetFieldData("t1305OutBlock", 'date', 0)
    print(to_append)

    XAQueryEventHandlerT8413.query_state = 0
    val, to_append_new, to_append_idx_new = getTotalPriceData(
        code, date_new, count-new_count, False, to_append, to_append_idx)
    return True, to_append_new, to_append_idx_new


if __name__ == '__main__':
    logIn()
    getCurrentPrice("078020")
    getCurrentPrice("005930")
    getFullPriceData("078020", '20100505', '20200808')
    # time.sleep(3)
    # getFullPriceData("005930", '20200901', '20200908')
    # # log.write(getJongmokCodeList(3))
    # time.sleep(3)
    # log.write(getFullPriceData('005930', '20190311', '20200319'))
    with open('./data/samplePriceData.json', 'w') as outfile:
        json.dump(getTotalPriceData('005930', '20190311', 1000),
                  outfile, indent=4, ensure_ascii=False)
