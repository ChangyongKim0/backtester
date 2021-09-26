import win32com.client
import pythoncom
import event_logger as log


class XSession:
    #     classmethod get_instance() 를 사용하여, instance 를 만들어야함.
    def __init__(self):
        self.login_state = 0

    def OnLogin(self, code, msg):  # event handler
        #         Login 이 성공적으로 이베스트 서버로 전송된후,
        #         로그인 결과에 대한 Login 이벤트 발생시 실행되는 event handler
        if code == "0000":
            log.write("로그인 ok")
            self.login_state = 1
        else:
            self.login_state = 2
            log.write(
                "로그인 fail.. code={0}, message={1}".format(code, msg))

    def api_login(self, id="cyklone", pwd="ckddyd!1", cert_pwd="cyklone001@"):  # id, 암호, 공인인증서 암호
        self.ConnectServer("hts.ebestsec.co.kr", 20001)
        is_connected = self.Login(id, pwd, cert_pwd, 0, False)  # 로그인 하기

        if not is_connected:  # 서버에 연결 안되거나, 전송 에러시
            log.write("로그인 서버 접속 실패... ")
            return

        while self.login_state == 0:
            pythoncom.PumpWaitingMessages()

    def account_info(self):
        #         """
        #         계좌 정보 조회
        #         """
        if self.login_state != 1:  # 로그인 성공 아니면, 종료
            return

        account_no = self.GetAccountListCount()

        log.write("계좌 갯수 = {0}".format(account_no))

        for i in range(account_no):
            account = self.GetAccountList(i)
            log.write("계좌번호 = {0}".format(account))

    @classmethod
    def get_instance(cls):
        # DispatchWithEvents로 instance 생성하기
        xsession = win32com.client.DispatchWithEvents(
            "XA_Session.XASession", cls)
        return xsession


def logIn():
    xsession = XSession.get_instance()
    xsession.api_login()

    xsession.account_info()
    log.write("Logged In Successfully.")


if __name__ == "__main__":
    logIn()
