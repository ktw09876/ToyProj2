# transform.py
import os
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, udf
from pyspark.sql.types import StringType

class Transform:
    """
    PySpark를 사용하여 Kafka의 원본 데이터를 JSON으로 가공하는 클래스
    """
    def __init__(self):
        self.spark = self._create_spark_session()
        # 원본 데이터를 JSON으로 변환하기 위한 헤더 정보, 데이터 종류에 따라 컬럼 종류가 달라짐
        self.TR_HEADERS = {
            'H0STASP0': [ #국내주식 실시간 호가
                '유가증권 단축 종목코드', '영업시간', '시간구분코드',
                '매도호가01', '매도호가02', '매도호가03', '매도호가04', '매도호가05', '매도호가06', '매도호가07', '매도호가08', '매도호가09', '매도호가10',
                '매수호가01', '매수호가02', '매수호가03', '매수호가04', '매수호가05', '매수호가06', '매수호가07', '매수호가08', '매수호가09', '매수호가10',
                '매도호가잔량01', '매도호가잔량02', '매도호가잔량03', '매도호가잔량04', '매도호가잔량05', '매도호가잔량06', '매도호가잔량07', '매도호가잔량08', '매도호가잔량09', '매도호가잔량10',
                '매수호가잔량01', '매수호가잔량02', '매수호가잔량03', '매수호가잔량04', '매수호가잔량05', '매수호가잔량06', '매수호가잔량07', '매수호가잔량08', '매수호가잔량09', '매수호가잔량10',
                '총매도호가 잔량', '총매수호가 잔량', '시간외 총매도호가 잔량', '시간외 총매수호가 잔량',
                '예상 체결가', '예상 체결량', '예상 거래량', '예상체결 대비', '부호', '예상체결 전일대비율',
                '누적거래량', '총매도호가 잔량 증감', '총매수호가 잔량 증감', '시간외 총매도호가 잔량 증감', '시간외 총매수호가 잔량 증감',
                '주식매매 구분코드'
            ],
            
            'H0STCNT0': [ #국내주식 실시간 체결가
                '유가증권단축종목코드','주식체결시간','주식현재가','전일대비부호','전일대비','전일대비율','가중평균주식가격','주식시가','주식최고가','주식최저가',
                '매도호가1','매수호가1','체결거래량','누적거래량','누적거래대금','매도체결건수','매수체결건수','순매수체결건수','체결강도','총매도수량','총매수수량',
                '체결구분','매수비율','전일거래량대비등락율','시가시간','시가대비구분','시가대비','최고가시간','고가대비구분','고가대비','최저가시간','저가대비구분','저가대비',
                '영업일자','신장운영구분코드','거래정지여부','매도호가잔량','매수호가잔량','총매도호가잔량','총매수호가잔량','거래량회전율','전일동시간누적거래량','전일동시간누적거래량비율',
                '시간구분코드','임의종료구분코드','정적VI발동기준가'
            ],
            
            'H0STANC0': [ #국내주식 실시간 예상체결가
                '유가증권단축종목코드','주식체결시간','주식현재가','전일대비구분','전일대비','등락율','가중평균주식가격','시가','고가','저가','매도호가','매수호가','거래량','누적거래량',
                '누적거래대금','매도체결건수','매수체결건수','순매수체결건수','체결강도','총매도수량','총매수수량','체결구분','매수비율','전일거래량대비등락율','시가시간','시가대비구분',
                '시가대비','최고가시간','고가대비구분','고가대비','최저가시간','저가대비구분','저가대비','영업일자','신장운영구분코드','거래정지여부','매도호가잔량1','매수호가잔량1',
                '총매도호가잔량','총매수호가잔량','거래량회전율','전일동시간누적거래량','전일동시간누적거래량비율','시간구분코드','임의종료구분코드'
            ]
        }

    def _create_spark_session(self):
        """ SparkSession을 생성하고 카프카 연동에 필요한 설정을 추가합니다. """
        try:
            spark = (SparkSession.builder
                     .appName("KafkaToJSONTransformer")
                     .master("local[*]") # 로컬의 모든 CPU 코어를 사용
                    #  .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
                     .getOrCreate())
            print("SparkSession 생성 성공")
            return spark
        except Exception as e:
            print(f"SparkSession 생성 실패: {e}")
            return None

    def _create_parser_udf(self):
        """
        데이터의 payload 부분을 JSON 문자열로 변환하는 UDF(사용자 정의 함수)를 생성합니다.
        """
        headers_map = self.TR_HEADERS # UDF 내부에서 사용할 수 있도록 변수에 할당

        def parse_payload_to_json(tr_id, payload_str):
            if tr_id not in headers_map or payload_str is None:
                return None
            
            headers = headers_map[tr_id]
            values = payload_str.split('^')

            if len(headers) != len(values):
                return None # 필드 개수가 맞지 않으면 null 반환
            
            doc = dict(zip(headers, values))
            return json.dumps(doc, ensure_ascii=False)

        return udf(parse_payload_to_json, StringType())

    def run(self):
        """
        스파크 스트리밍 작업을 시작합니다.
        """
        if not self.spark:
            print("스파크 세션이 없어 작업을 시작할 수 없습니다.")
            return

        # 1. Kafka 소스에서 데이터 스트림 읽기
        kafka_df = (self.spark.readStream
                    .format("kafka")
                    .option("kafka.bootstrap.servers", "kafka:29092")
                    .option("subscribePattern", "H0ST.*") # H0ST로 시작하는 모든 토픽 구독
                    .option("startingOffsets", "earliest")
                    .load())

        # UDF 생성
        parse_udf = self._create_parser_udf()

        # 2. 데이터 변환
        processed_df = (kafka_df
                        # Kafka 메시지의 value를 문자열로 변환
                        .withColumn("raw_string", col("value").cast(StringType()))
                        # '|'로 분리하여 각 필드 추출
                        .withColumn("parts", split(col("raw_string"), "\\|"))
                        .withColumn("tr_id", col("parts").getItem(1))
                        .withColumn("payload", col("parts").getItem(3))
                        # UDF를 사용하여 payload를 JSON 문자열로 변환
                        .withColumn("json_data", parse_udf(col("tr_id"), col("payload")))
                        # 필요한 데이터만 선택 (토픽명, 메시지 키, JSON 데이터)
                        .select(
                            col("topic"), 
                            col("key").cast(StringType()).alias("stock_code"), 
                            col("json_data")
                        )
                        # JSON 변환에 실패한 데이터는 제외
                        .filter(col("json_data").isNotNull()))

        # 3. 변환된 데이터를 콘솔에 출력 (Sink)
        # 실제 운영 시에는 .format("kafka"), .format("mongodb") 등으로 변경 가능
        query = (processed_df.writeStream
                 .outputMode("update")
                 .format("console")
                 .option("truncate", "false") # 내용을 자르지 않고 모두 표시
                 .start())

        print("Spark Streaming 작업 시작. 콘솔에서 결과를 확인하세요.")
        query.awaitTermination()

if __name__ == "__main__":
    transformer = Transform()
    transformer.run()