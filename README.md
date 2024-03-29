# Stocks_ProJ
Data_Engineering DEV course 4th Project

## 프로젝트 주제
주식, 경제지표 API를 활용한 주가 및 경제지표 상관관계 시각화

## 프로젝트 개요

### 1. 내용
주식 API를 활용하여 2021년 1월 1일부터 현재까지 시가총액 상위 100개 종목의 주가 정보를 수집하고, 

이 기간 동안의 뉴스를 크롤링하여 분석하고, 

환율과 자원 시장의 주요 지표인 유가, 금, 구리 등의 정보도 수집하여, 이들 간의 상관관계를 분석하여 시각화하는 프로젝트입니다.

### 2. 기간
  2024.02.12(월) ~ 2024.03.08(금)

### 3. 활용 데이터
   
  | 데이터 | URL |
  |---|---|
  | 주식 데이터 | https://apiportal.koreainvestment.com/apiservice/apiservice-domestic-stock-quotations#L_a08c3421-e50f-4f24-b1fe-64c12f723c77 |
  | 금,구리,유가 데이터 | yfinance |
  | 환율 데이터 |  네이버 페이 증권 페이지 |
  | 뉴스 데이터 | 네이버 증권 뉴스 페이지 |

### 4. 팀원 역할 소개
   
  |이름||역할|기여도|
  | ---|---| ---| ---|
  |박진영 |@jypark92 | AWS 리소스(EC2, S3, RDS) 세팅, 대시보드 세팅, 스키마 설계, 뉴스·환율 데이터 EDA·ETL·ELT, 유가·금 시세·구리  시세 데이터 ETL·ELT, 차트 생성, CI/CD, 유닛 테스트 코드 작성 | 50%|
  |이상진 |@MineTime23 | Github 세팅, 스키마 설계, E-R 다이어그램 작성, 주식 데이터 EDA·ETL·ELT, 유가·금 시세·구리  시세 데이터 EDA |50%|

### 5. 보고서

https://www.notion.so/db4c5534037946878acb6c6815c9897c

## 프로젝트 구현
### 구조
- 프로젝트 아키텍쳐
    ![Image](https://github.com/MineTime23/Image/blob/master/Untitled%20(2).png)
- ERD
    ![Image](https://github.com/MineTime23/Image/blob/master/ERD.png)

## 대시 보드
  ![Image](https://github.com/MineTime23/Image/blob/master/price%20dashboard.png)
  ![Image](https://github.com/MineTime23/Image/blob/master/finance%20dashboard.png)
  ![Image](https://github.com/MineTime23/Image/blob/master/amount%20dashboard.png)
  ![Image](https://github.com/MineTime23/Image/blob/master/news%20dashboard.png)
  ![Image](https://github.com/MineTime23/Image/blob/master/top10%20dashboard.png)
