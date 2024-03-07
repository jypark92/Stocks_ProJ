import unittest
from airflow.models import DagBag
from dags import Exchange_Rate_to_S3 as ers
from dags import News_to_Analytics as na

class TestCases(unittest.TestCase):
    def setUp(self):
        self.dagbag = DagBag(include_examples=False)

    def test_dag_integrity(self):
        errors_dict = self.dagbag.import_errors.items()

        self.assertEqual(0, len(errors_dict))

    def test_exchange_rate_to_s3_transform(self):
        data_list = [
                {"date": "2024.02.23", "exchange_rate": "1,332.50", "change": "+3.50", "symbol": "USDKRW"},
                {"date": "2024.02.22", "exchange_rate": "1,332.50", "change": "+3.50", "symbol": "USDKRW"},
                {"date": "2024.02.21", "exchange_rate": "1,332.50", "change": "+3.50", "symbol": "USDKRW"}
            ]
        date_set = {"2024.02.21", "2024.02.22", "2024.02.23"}

        self.assertEqual(set(ers.transform(data_list)), date_set)
    
    def test_news_to_analytics_normalize(self):
        text = '[위클리 건강] 건강관리·돌봄에 뜨는 스마트링…삼성·애플도 큰 관심'
        return_text = '건강관리 돌봄에 뜨는 스마트링 삼성 애플도 큰 관심'

        self.assertEqual(na.normalize(text), return_text)
    

if __name__ == "__main__":
    unittest.main()