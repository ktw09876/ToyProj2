# -*- coding: utf-8 -*-
import os
import sys
import json
import time
import requests
import asyncio
import traceback
import websockets
import configparser as parser
from kafka import KafkaProducer

class KisApiProducer:
    """
    한국투자증권 WebSocket API로부터 실시간 주식 데이터를 수신하여
    원본(raw) 데이터 그대로 Kafka로 전송하는 Producer 클래스.
    """
    def __init__(self, config_path: str):
        """
        클래스 초기화. 설정 파일 로드 및 Kafka Producer 생성을 담당합니다.
        """
        # self.appkey, self.secretkey, self.base_url, self.end_url = self._read_conf(config_path)
        self.config = self._read_conf(config_path)
        self.producer = self._init_kafka()
        self.ws_url = 'ws://ops.koreainvestment.com:31000' # 모의투자

        self.code_list = [
            # 삼성전자(005930)
             ['1', 'H0STASP0', '005930'] # 실시간 호가
            ,['1', 'H0STCNT0', '005930'] # 실시간 체결가
            ,['1', 'H0STANC0', '005930'] # 실시간 예상체결가

            # SK하이닉스(000660)
            ,['1', 'H0STASP0', '000660']
            ,['1', 'H0STCNT0', '000660']
            ,['1', 'H0STANC0', '000660']

            # 카카오(035720)
            ,['1', 'H0STASP0', '035720']
            ,['1', 'H0STCNT0', '035720']
            ,['1', 'H0STANC0', '035720']
        ]

    def _read_conf(self, ini_path: str):
        if not os.path.exists(ini_path):
            raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {ini_path}")
        
        config = parser.ConfigParser(inline_comment_prefixes = ('#', ';')) #inline_comment_prefixes 를 지정하지 않으면 '#' or ';'로 시작하는 경우를 제외하고 값 뒤에 오는 주석을 인식 못 함
        with open(ini_path, 'r', encoding = 'utf-8') as f: #한글 주석을 인식하기 위한 옵션
            config.read_file(f)

        return config

    def _init_kafka(self):
        try:
            kafka_conf = self.config['KAFKA']
            ['bootstrap_servers']
            producer = KafkaProducer(
                bootstrap_servers = kafka_conf['bootstrap_servers'],
                value_serializer = lambda v: str(v).encode('utf-8')
            )
            print("Kafka Producer 초기화 성공")
            return producer
        
        except Exception as e:
            print(f"Kafka Producer 초기화 실패: {e}")
            return None

    def _get_approval(self):
        appr_conf = self.config['APPROVAL']
        headers = {
            "content-type": "application/json"
        }
        body = {
            "grant_type": "client_credentials",
            "appkey": appr_conf['appkey'],
            "secretkey": appr_conf['secretkey']
        }
        url = f"{appr_conf['base_url']}{appr_conf['end_url']}"
        try:
            time.sleep(0.05)
            response = requests.post(url, headers = headers, data = json.dumps(body))
            res = response.json()
            approval_key = res.get('approval_key')

            if approval_key:
                print(f"Approval Key 발급 성공: {approval_key[:10]}...")
            else:
                print(f"Approval Key 발급 실패: {res}")
            return approval_key
        
        except Exception as e:
            print(f"Approval Key 발급 중 예외 발생: {e}")
            return None

    async def run(self):
        """
        메인 실행 로직. 웹소켓에 접속하여 데이터를 받아 Kafka로 전송합니다.
        """
        if not self.producer:
            print("Kafka Producer가 초기화되지 않아 실행을 중단합니다.")
            return

        g_approval_key = self._get_approval()

        if not g_approval_key:
            return

        senddata_list = []
        for tr_type, tr_id, tr_key in self.code_list:
            temp_data = {
                "header": {
                    "approval_key": g_approval_key,
                    "custtype": "P",
                    "tr_type": tr_type,
                    "content-type": "utf-8"
                },
                "body": {
                    "input": {
                        "tr_id": tr_id,
                        "tr_key": tr_key
                    }
                }
            }
            senddata_list.append(json.dumps(temp_data))

        async with websockets.connect(self.ws_url, ping_interval = 60) as websocket:
            print("WebSockets 연결 성공")
            for senddata in senddata_list:
                await websocket.send(senddata)
                print(f"Sent Command: {senddata}")
                await asyncio.sleep(0.1)

            while True:
                try:
                    data = await websocket.recv()

                    if data[0] == '0': # 실시간 시세 데이터만 처리
                        recvstr = data.split('|')
                        tr_id = recvstr[1]
                        
                        # 종목코드를 Key로 추출하는 로직은 그대로 유지
                        message_key = None
                        if len(recvstr) > 3 and '^' in recvstr[3]:
                            stock_code = recvstr[3].split('^')[0]
                            if 0 < len(stock_code) < 10:
                                message_key = stock_code.encode('utf-8')
                        
                        # 원본 데이터(data)를 그대로 Kafka로 전송
                        self.producer.send(topic=tr_id, key=message_key, value=data)
                        print(f"Sent to Kafka topic '{tr_id}': {data[:80]}...")

                    elif data[0] == '{': # PINGPONG 또는 응답 메시지 처리
                        if "PINGPONG" in data:
                            await websocket.pong(data)
                            # print("Sent Pong") # 너무 자주 출력되므로 주석 처리
                        else:
                            print(f"Received Info: {data}")
                    
                except websockets.exceptions.ConnectionClosed:
                    print("웹소켓 연결이 종료되었습니다. 재접속을 시도합니다.")
                    raise
                except Exception as e:
                    print(f"처리 중 오류 발생: {e}, Data: {data}")

async def main():
    config_path = '/app/work_space/websocket/setting/setting.ini'
    while True:
        try:
            producer_app = KisApiProducer(config_path)
            await producer_app.run()
        except FileNotFoundError as e:
            print(f"설정 파일을 찾을 수 없습니다: {e}. 프로그램을 종료합니다.")
            break
        except Exception as e:
            print(f"메인 루프에서 예외 발생: {e}")
            print("5초 후 재연결을 시도합니다...")
            await asyncio.sleep(5)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("사용자에 의해 프로그램이 종료되었습니다.")