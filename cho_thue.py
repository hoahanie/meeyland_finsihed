import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
import urllib
import json
import datetime
from scrapy import Request

class chothueSpider(scrapy.Spider):
    name = 'chothue'
    base_url = 'https://api.meeyland.com/api/search'
    headers = {
        'authority': 'api.meeyland.com',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'accept': 'application/json, text/plain, */*',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.193 Safari/537.36',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://meeyland.com',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': 'https://meeyland.com/cho-thue-nha-dat',
        'accept-language': 'en-US,en;q=0.9,vi;q=0.8'
        }
    # custom_settings = {
    #     # uncomment to set accordingly
    #     "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
    #     "DOWNLOAD_DELAY": 0.5    # 250 ms of delay
    # }
    def start_requests(self):
        # filename = './output/hochiminh_muaban' + datetime.datetime.today().strftime('%Y-%m-%d-%H-%M') + '.jsonl'
        with open ('backup_city.json',"r") as f:
            data = json.loads(f.read())
            for province in data:
                province_id = list(province.keys())[0]
                temp = province[province_id]
                seo_url = temp.lower().replace(" ","-")
                filename = './output/chothue/' + seo_url+ '_' + datetime.datetime.today().strftime('%Y-%m-%d-%H-%M') + '.jsonl'

                para = {
                        "category":"5deb722db4367252525c1d11",
                        "filter":"{\"attributes\":{\"5dfb2acdd5e511385e90df86\":[\""+ province_id+"\"]},\"seoUrl\":\""+ seo_url+"\",\"date\":{\"startDate\":\"2001-01-19T17:00:00.000Z\",\"endDate\":\"2020-11-11T16:59:59.999Z\"}}",
                        "sort":"{\"createdDate\":-1}",
                        "skip":"0",
                        "limit":"20",
                        "search":""
                    }

                yield Request(
                    url=self.base_url,
                    method='POST',
                    dont_filter=True,
                    headers=self.headers,
                    body=json.dumps(para),
                    meta= {
                        "province_id": province_id,
                        'filename': filename,
                        "seo_url": seo_url
                        },
                    callback=self.parse_ads
                    )
                # break

    def parse_ads(self, response):
        data = json.loads(response.body)
        province_id = response.meta["province_id"]
        filename = response.meta["filename"]
        seo_url = response.meta["seo_url"]
        total_ads = data["total"]
        number_page = round(total_ads/20)
        for page in range(1,number_page+1):
            para = {
                    "category":"5deb722db4367252525c1d11",
                    "filter":"{\"attributes\":{\"5dfb2acdd5e511385e90df86\":[\""+ province_id+"\"]},\"seoUrl\":\""+ seo_url+"\",\"date\":{\"startDate\":\"2001-01-19T17:00:00.000Z\",\"endDate\":\"2020-11-11T16:59:59.999Z\"}}",
                    "sort":"{\"createdDate\":-1}",
                    "skip":str((page-1)*20),
                    "limit":"20",
                    "search":""
                }

            yield response.follow(
                url=self.base_url,
                method='POST',
                dont_filter=True,
                headers=self.headers,
                body=json.dumps(para),
                meta= {
                    # "dis_id": dis_id,
                    'filename': filename
                    # "seo_url": seo_url
                },
                callback=self.parse_page
                )
    def parse_page(self,response):
        data = json.loads(response.body)
        filename = response.meta["filename"]
        with open(filename,"a") as f:
            for item in data["articles"]:
                f.write(json.dumps(item) + '\n')

if __name__ == '__main__':
    # run scraper
    process = CrawlerProcess()
    process.crawl(chothueSpider)
    process.start()
