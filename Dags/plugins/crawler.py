import re
from bs4 import BeautifulSoup

class NewsScraper:
    def __init__(self, ticker, day):
        self.ticker = ticker
        self.is_today = True
        self.total_set = set()
        self.day = day

    async def get_article_url(self, soup):
        article_id, office_id = re.findall(r'article_id=[0-9]*|office_id=[0-9]*', soup.find("td", "title").find("a")["href"])
        article_id = article_id[11:]
        office_id = office_id[10:]
        url = f"https://n.news.naver.com/mnews/article/{office_id}/{article_id}"
        return article_id, url

    async def get_article_info(self, date, url, session):
        async with session.get(url) as response:
            if response.status != 200:
                print(f"Failed to fetch article from {url}. Status code: {response.status}")
                return 
            res_text = await response.text()
            soup = BeautifulSoup(res_text, "html.parser")
            title_soup = soup.find("h2", id="title_area")
            if title_soup:
                title = title_soup.text.strip()
            elif soup.find("h4", "title"):
                title = soup.find("h4", "title").text.strip()
            else:
                title = soup.find("h2", "end_tit").text.strip()
            result = {
                "title": title,
                "published_time": date,
                "ticker_symbol": self.ticker
            }                 
                
        return result
    
    async def get_articles_soup(self, page, session):
        async with session.get(f"https://finance.naver.com/item/news_news.naver?code={self.ticker}&page={page}&sm=title_entity_id.basic&clusterId=") as response:
            if response.status != 200:
                print(f"Failed to fetch articles page {page}. Status code: {response.status}")
                return []
            res_text = await response.text()
            soup = BeautifulSoup(res_text, "html.parser")
            article_soups = soup.find("tbody").find_all("tr")
        return article_soups
    
    async def get_article_data(self, soup, session):
        if not soup.find("td", "title"):
            return 
        date = soup.find("td", "date").text.strip()
        if date[:10] != self.day:
            self.is_today = False
            return 
        article_id, url = await self.get_article_url(soup)
        if article_id in self.total_set:
            return 1
        self.total_set.add(article_id)
        result = await self.get_article_info(date, url, session)
        return result
    
    async def get_news_data(self, session):
        results = []
        for i in range(1, 11):
            for article in await self.get_articles_soup(i, session):
                result = await self.get_article_data(article, session)
                if not result:
                    break
                elif result == 1:
                    continue
                results.append(result)
            if not self.is_today:
                break
        return results
    
class ExchangeRateScraper:
    def __init__(self, symbol, end_date, start_date=None):
        self.symbol = symbol
        self.total_set = set()
        self.start_date = start_date
        self.end_date = end_date
        self.sign_dict = {
            "보합": "",
            "상승": "+",
            "하락": "-"
        }
        self.is_break = False
        
    async def get_rows_soup(self, page, session):
        async with session.get(f"https://finance.naver.com/marketindex/exchangeDailyQuote.naver?marketindexCd=FX_{self.symbol}&page={page}") as response:
            if response.status != 200:
                print(f"Failed to fetch page {page}. Status code: {response.status}")
                return []
            res_text = await response.text()
            soup = BeautifulSoup(res_text, "html.parser")
            soups = soup.find("tbody").find_all("tr")
        return soups
    
    async def get_row_data(self, soup):
        date = soup.find_all("td", "date")
        if not date:
            return 
        date = date[0].text.strip()
        if self.start_date:
            if date < self.start_date:
                self.is_break = True
                return 
        elif date < self.end_date:
            self.is_break = True
            return 
        if date in self.total_set:
            return 1
        self.total_set.add(date)
        num_data = soup.find_all("td", "num")
        sign_text = num_data[1].find("img")["alt"]
        result = {
            "date": date,
            "exchange_rate": num_data[0].text,
            "change": self.sign_dict[sign_text] + num_data[1].text.strip(),
            "symbol": self.symbol
            }     
        return result
    
    async def get_news_data(self, session):
        results = []
        for i in range(1, 101):
            for row in await self.get_rows_soup(i, session):
                result = await self.get_row_data(row)
                if not result:
                    break
                elif result == 1:
                    continue
                results.append(result)
            if self.is_break:
                break
        return results