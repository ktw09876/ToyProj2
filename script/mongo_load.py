# -*- coding: utf-8 -*-
import os
from datetime import datetime
import time
import configparser as parser
from kafka import KafkaConsumer
from pymongo import MongoClient

class LoadToMongo:
    """
    Kafka 토픽에서 원본(raw) 데이터를 구독하여 MongoDB에 그대로 저장하는 클래스.
    """
    def __init__(self, config_path: str):
        self.config = self._read_conf(config_path)
        
        # 구독할 토픽 목록 (필요에 따라 .ini 파일에서 읽어오도록 수정 가능)
        self.topics_to_subscribe = ['H0STASP0', 'H0STCNT0', 'H0STANC0']
        
        self.mongo_client, self.db = self._init_mongo()
        self.consumer = self._init_kafka_consumer()

    def _read_conf(self, ini_path: str):
        if not os.path.exists(ini_path):
            raise FileNotFoundError(f"설정 파일을 찾을 수 없습니다: {ini_path}")
        
        config = parser.ConfigParser(inline_comment_prefixes = ('#', ';')) #inline_comment_prefixes 를 지정하지 않으면 '#' or ';'로 시작하는 경우를 제외하고 값 뒤에 오는 주석을 인식 못 함
        with open(ini_path, 'r', encoding = 'utf-8') as f: #한글 주석을 인식하기 위한 옵션
            config.read_file(f)

        return config

    def _init_mongo(self):
        try:
            mongo_conf = self.config['MONGODB']
            client = MongoClient(mongo_conf['uri'])
            db = client[mongo_conf['db_name']]
            
            print("MongoDB 연결 성공")
            return client, db
        
        except Exception as e:
            print(f"MongoDB 연결 실패: {e}")
            return None, None

    def _init_kafka_consumer(self):
        try:
            kafka_conf = self.config['KAFKA']
            consumer = KafkaConsumer(
                *self.topics_to_subscribe,
                bootstrap_servers = kafka_conf['bootstrap_servers'],
                group_id = 'mongo-raw-loader-group', # 그룹 ID 변경
                auto_offset_reset = 'earliest',
                value_deserializer = lambda v: v.decode('utf-8')
            )
            print(f"Kafka Consumer 초기화 성공. 구독 토픽: {self.topics_to_subscribe}")
            return consumer
        
        except Exception as e:
            print(f"Kafka Consumer 초기화 실패: {e}")
            return None

    def run(self):
        """
        Kafka로부터 메시지를 계속 읽어와 MongoDB에 저장하는 메인 루프.
        """
        if self.consumer is None or self.db is None:
            print("초기화 실패로 실행을 중단합니다.")
            return

        print("MongoDB 원본 데이터 적재를 시작합니다...")
        try:
            for message in self.consumer:
                try:
                    # Kafka 메시지에서 토픽 이름과 원본 데이터(값)를 가져옴
                    topic = message.topic
                    raw_data_string = message.value

                    # --- 타임스탬프 변환 로직 ---
                    # 1. Kafka 타임스탬프 (밀리초 -> 날짜/시간 문자열)
                    # 1000으로 나누어 초 단위로 변경 후 변환
                    kafka_dt = datetime.fromtimestamp(message.timestamp / 1000)
                    kafka_time_str = kafka_dt.strftime('%Y-%m-%d %H:%M:%S')

                    # 2. MongoDB 적재 시간 (초 -> 날짜/시간 문자열)
                    insert_time_unix = time.time()
                    insert_dt = datetime.fromtimestamp(insert_time_unix)
                    insert_time_str = insert_dt.strftime('%Y-%m-%d %H:%M:%S')
                    # --- 변환 로직 끝 ---

                    # MongoDB에 저장할 문서(Document) 생성
                    document_to_save = {
                        "raw_data": raw_data_string,
                        "topic": topic,
                        "partition": message.partition,
                        "offset": message.offset,
                        "kafka_timestamp": kafka_time_str,  # 변환된 문자열로 저장
                        "insert_time": insert_time_str,   # 변환된 문자열로 저장

                        # (선택사항) 나중에 정확한 계산을 위해 원본 숫자 타임스탬프도 함께 저장하는 것을 추천
                        "kafka_timestamp_ms": message.timestamp,
                        "insert_time_unix": insert_time_unix
                    }

                    # 토픽 이름을 컬렉션 이름으로 사용
                    collection = self.db[topic]
                    collection.insert_one(document_to_save)
                    print(f"Saved to MongoDB -> Collection: {topic}, Offset: {message.offset}")

                except Exception as e:
                    print(f"메시지 처리 중 오류 발생: {e}, 데이터: {message.value}")

        except KeyboardInterrupt:
            print("사용자에 의해 데이터 적재가 중단되었습니다.")
        finally:
            if self.consumer: self.consumer.close()
            if self.mongo_client: self.mongo_client.close()
            print("Kafka Consumer 및 MongoDB 연결이 종료되었습니다.")

# -----------------------------------------------------------------------------
# - Name : main
# - Desc : 메인 실행 함수
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    config_path = '/app/work_space/websocket/setting/setting.ini'
    try:
        loader_app = LoadToMongo(config_path)
        loader_app.run()
    except Exception as e:
        print(f"Loader 애플리케이션 실행 실패: {e}")