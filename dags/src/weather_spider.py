import json
from datetime import datetime
import scrapy

# web = requests.get('https://openweathermap.org/city/1580578')
# soup = BeautifulSoup(web.content)

class Scraper(scrapy.Spider):
    name = 'weatherdata'
    start_urls = [
        'https://openweathermap.org/city/1580578' # Ho Chi Minh City, Vietnam
    ]
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.5",
        "Connection": "keep-alive",
        "Host": "openweathermap.org",
        "Referer": "https://openweathermap.org/city/1566083",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Site": "same-origin",
        "User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:102.0) Gecko/20100101 Firefox/102.0"
    }

    def __init__(self, ti=None, name=None, **kwargs) -> None:
        super().__init__(name, **kwargs)
        self.ti = ti
        self.bucket = 'weather-data'
    def parse(self, response, **kwargs):
        url = 'https://openweathermap.org/data/2.5/weather?id=1566083&appid=439d4b804bc8187953eb36d2a8c26a02'

        request = scrapy.Request(url, callback=self.parse_api, headers=self.headers)
        yield request

    def parse_api(self, response):
        data = json.loads(response.body)
        
        # collect necessary items
        if 'rain' in data:
            rain = data['rain']
        else:
            rain = {}
        weather = {
            'main': data['weather'][0]['main'],
            'description': data['weather'][0]['description']
        }
        temp = {
            'real_temp': data['main']['temp'],
            'feels_like': data['main']['feels_like']
        }
        humidity = data['main']['humidity']
        wind = data['wind']
        time = str(datetime.now())
        new_data = {
            'measurement': 'weather_indices',
            'tags': {
                'description': weather['main'],
                'city': data['name'],
                'cityID': data['id']
            },
            'fields': {    
                'weather': weather, 
                'rain': rain,
                'temp': temp,
                'humidity': humidity,
                'wind': wind
            },
            'time': time
        }

        self.ti.xcom_push('weather_data', json.dumps(new_data))
        yield new_data