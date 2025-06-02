import requests
import configparser as parser
import json
import asyncio
import time

#접속키의 유효기간은 24시간이지만, 접속키는 세션 연결 시 초기 1회만 사용하기 때문에 접속키 인증 후에는 세션종료되지 않는 이상 접속키 신규 발급받지 않으셔도 365일 내내 웹소켓 데이터 수신하실 수 있습니다.
class ApprovalClient:
    def __init__(self):
        ###접속 정보 설정
        self.appkey, self.secretkey, self.base_url, self.end_url = self.read_conf(r'D:\tool\work_space\websocket\setting\setting.ini')

    #경로를 넘겨 받아 setting.ini를 읽어오는 메서드
    def read_conf(self, ini_path: str) -> list:
        config = parser.ConfigParser(inline_comment_prefixes = ('#', ';')) #inline_comment_prefixes 를 지정하지 않으면 '#' or ';'로 시작하는 경우를 제외하고 값 뒤에 오는 주석을 인식 못 함
        with open(ini_path, 'r', encoding = 'utf-8') as f: #한글 주석을 인식하기 위한 옵션
            config.read_file(f)

        appkey = config['approval']['appkey']
        secretkey = config['approval']['secretkey']
        base_url = config['approval']['base_url']
        end_url = config['approval']['end_url']

        return appkey, secretkey, base_url, end_url

    #content_type, grant_type, appkey, secretkey, base_url, end_url을 넘겨 받아 api를 호출하는 메서드
    def get_approval(self, appkey, secretkey, base_url, end_url):
        headers = {
            "content-type":"application/json"
        }
        body = {
             "grant_type":"client_credentials"
            ,"appkey":appkey
            ,"secretkey":secretkey
        }

        #api 호출
        try:
            time.sleep(0.05)
            response = requests.post(f'{base_url}{end_url}', headers = headers, data = json.dumps(body))
            print(response.status_code) #코드 확인
            print(response.text) #값 확인
            res = response.json() #전체 값을 json 형태로 파싱
            
            return res['approval_key'] #그 중 approval_key 에 해당하는 값만 돌려준다
        except Exception as e:
            print(f"  종류: {type(e).__name__}")
            print(f"  메시지: {str(e)}")




class StockExecutionPrice:
    def __init__(self, key):
        ###접속 정보 설정
        self.key = key
        self.custtype, self.tr_type, self.base_url, self.end_url = self.read_conf(r'D:\tool\work_space\websocket\setting\setting.ini')

    #경로를 넘겨 받아 setting.ini를 읽어오는 메서드
    def read_conf(self, ini_path: str) -> list:
        config = parser.ConfigParser(inline_comment_prefixes = ('#', ';')) #inline_comment_prefixes 를 지정하지 않으면 '#' or ';'로 시작하는 경우를 제외하고 값 뒤에 오는 주석을 인식 못 함
        with open(ini_path, 'r', encoding = 'utf-8') as f: #한글 주석을 인식하기 위한 옵션
            config.read_file(f)

        custtype = config['H0STCNT0']['custtype']
        tr_type = config['H0STCNT0']['tr_type']
        base_url = config['H0STCNT0']['base_url']
        end_url = config['H0STCNT0']['end_url']

        return custtype, tr_type, base_url, end_url
    


def main():
    #Approval_key
    app = ApprovalClient()
    Approval_key = app.get_approval(app.appkey, app.secretkey, app.base_url, app.end_url)


if __name__ == '__main__':
    main()




