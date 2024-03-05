import unittest
from airflow.models import DagBag
from dags import Exchange_Rate_to_S3 as ers
from dags import exchange_rate_to_raw_data as err
from dags import News_to_Analytics as na
from dags import news_to_raw_data as nr
from dags import Raw_Materials_to_S3 as rms

class TestCases(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag()

    def test_dag_integrity(self):
        dags_with_errors = []
        for dag_id, dag in self.dagbag.dags.items():
            dag_test = self.dagbag.process_file(dag.fileloc)
            if dag_test.import_errors:
                dags_with_errors.append(dag_id)

        self.assertEqual([], dags_with_errors)

    def test_exchange_rate_to_s3_transform(self):
        data_list = [
                {"date": "2024.02.23", "exchange_rate": "1,332.50", "change": "+3.50", "symbol": "USDKRW"},
                {"date": "2024.02.22", "exchange_rate": "1,332.50", "change": "+3.50", "symbol": "USDKRW"},
                {"date": "2024.02.21", "exchange_rate": "1,332.50", "change": "+3.50", "symbol": "USDKRW"}
            ]
        date_list = ["2024.02.21", "2024.02.22", "2024.02.23"]

        self.assertEqual(ers.transform(data_list), date_list)

    def test_exchange_rate_to_raw_data_transform(self):
        records = {'date': {0: '2024.02.23', 1: '2024.02.23', 2: '2024.02.23'},
                   'exchange_rate': {0: '1,332.50', 1: '1,442.03', 2: '885.32'},
                   'change': {0: '+3.50', 1: '+4.72', 2: '+1.94'},
                   'symbol': {0: 'USDKRW', 1: 'EURKRW', 2: 'JPYKRW'}}
        transform_data = {'date': {0: '2024-02-23', 1: '2024-02-23', 2: '2024-02-23'},
                   'exchange_rate': {0: 1332.5, 1: 1442.03, 2: 885.32},
                   'change': {0: 3.5, 1: 4.72, 2: 1.94},
                   'symbol': {0: 'USDKRW', 1: 'EURKRW', 2: 'JPYKRW'}}
        
        self.assertEqual(err.transform(records), transform_data)
    
    def test_news_to_analytics_normalize(self):
        text = '[위클리 건강] 건강관리·돌봄에 뜨는 스마트링…삼성·애플도 큰 관심'
        return_text = '건강관리 돌봄에 뜨는 스마트링 삼성 애플도 큰 관심'

        self.assertEqual(na.normalize(text), return_text)
    
    def test_news_to_raw_data_transform(self):
        records = {'title': {0: '삼성 반도체 경영진은 일본에서 무엇을 보고 왔을까? [강해령의 하이엔드 테크] <끝·EUV 소재 편>',
                             1: '이재용, 삼성전자 등기이사 복귀 안한다 外[금주의 산업계 이슈]',
                             2: "삼성 '갤럭시 AI' 왕조 온다…폰·워치·버즈·태블릿 모두 AI 진화"},
                   'published_time': {0: '2024.02.24 09:01',
                                      1: '2024.02.24 09:00',
                                      2: '2024.02.24 08:00'},
                   'ticker_symbol': {0: '005930', 1: '005930', 2: '005930'}}
        transform_data = {'title': {0: '삼성 반도체 경영진은 일본에서 무엇을 보고 왔을까? [강해령의 하이엔드 테크] <끝·EUV 소재 편>',
                                    1: '이재용, 삼성전자 등기이사 복귀 안한다 外[금주의 산업계 이슈]',
                                    2: '삼성 갤럭시 AI 왕조 온다…폰·워치·버즈·태블릿 모두 AI 진화'},
                          'published_time': {0: '2024-02-24 09:01',
                                             1: '2024-02-24 09:00',
                                             2: '2024-02-24 08:00'},
                          'ticker_symbol': {0: '005930', 1: '005930', 2: '005930'}}
        
        self.assertEqual(nr.transform(records), transform_data)

    def test_raw_materials_to_s3_transform(self):
        records = {'gold': {'Date': {0: '2021-01-04', 1: '2021-01-05', 2: '2021-01-06'},
                            'Open': {0: 1912.199951171875, 1: 1941.699951171875, 2: 1952.0},
                            'High': {0: 1945.0999755859375, 1: 1952.699951171875, 2: 1959.9000244140625},
                            'Low': {0: 1912.199951171875, 1: 1941.300048828125, 2: 1901.5},
                            'Close': {0: 1944.699951171875, 1: 1952.699951171875, 2: 1906.9000244140625},
                            'Volume': {0: 154, 1: 113, 2: 331}},
                   'copper': {'Date': {0: '2021-01-04', 1: '2021-01-05', 2: '2021-01-06'},
                              'Open': {0: 3.559000015258789, 1: 3.5899999141693115, 2: 3.6505000591278076},
                              'High': {0: 3.6005001068115234, 1: 3.6554999351501465, 2: 3.690500020980835},
                              'Low': {0: 3.5455000400543213, 1: 3.5899999141693115, 2: 3.640000104904175},
                              'Close': {0: 3.552999973297119, 1: 3.640500068664551, 2: 3.6500000953674316},
                              'Volume': {0: 864, 1: 512, 2: 842}},
                   'oil': {'Date': {0: '2021-01-04', 1: '2021-01-05', 2: '2021-01-06'},
                           'Open': {0: 48.400001525878906, 1: 47.380001068115234, 2: 49.81999969482422},
                           'High': {0: 49.83000183105469, 1: 50.20000076293945, 2: 50.939998626708984},
                           'Low': {0: 47.18000030517578, 1: 47.2400016784668, 2: 49.47999954223633},
                           'Close': {0: 47.619998931884766,
                                     1: 49.93000030517578,
                                     2: 50.630001068115234},
                           'Volume': {0: 528525, 1: 643191, 2: 509365}}}
        date_dict = {'gold': ['2021-01-04', '2021-01-05', '2021-01-06'],
                     'copper': ['2021-01-04', '2021-01-05', '2021-01-06'],
                     'oil': ['2021-01-04', '2021-01-05', '2021-01-06']}
        
        self.assertEqual(rms.transform(records), date_dict)

if __name__ == "__main__":
    unittest.main()