import scrapy
from scrapy import Request
import json

class meeylanSpider(scrapy.Spider):
    name = "hochiminh"
    url = 'https://api.meeyland.com/api/search'

    headers = {
        'authority': 'api.meeyland.com',
        'pragma': 'no-cache',
        'cache-control': 'no-cache',
        'accept': 'application/json, text/plain, */*',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
        'content-type': 'application/json;charset=UTF-8',
        'origin': 'https://meeyland.com',
        'sec-fetch-site': 'same-site',
        'sec-fetch-mode': 'cors',
        'sec-fetch-dest': 'empty',
        'referer': "https://meeyland.com/mua-ban-nha-dat",
        'accept-language': 'en-US,en;q=0.9',
    }
    def start_requests(self):

        temp = "Ho Chi Minh"
        city_id = "5e5501caeb80a7245175de0c"
        seo_url = temp.lower().replace(" ","-")
        data = '{"category":"5deb722db4367252525c1d00","filter":"{\\"attributes\\":{\\"5dfb2acdd5e511385e90df86\\":[\\"'+ city_id+'\\"]},\\"seoUrl\\":\\"'+seo_url+'\\",\\"date\\":{\\"startDate\\":\\"2001-01-19T17:00:00.000Z\\",\\"endDate\\":\\"2020-11-06T16:59:59.999Z\\"}}","sort":"{\\"createdDate\\":-1}","skip":0,"limit":20,"search":""}'
        # data = "{"category":"5deb722db4367252525c1d00","filter":"{\\"attributes\\":{\\"5dfb2acdd5e511385e90df86\\":[\\" + city_id + "\\"]},\\"seoUrl\\":\\"ho-chi-minh\\",\\"date\\":{\\"startDate\\":\\"2019-10-31T17:00:00.000Z\\",\\"endDate\\":\\"2020-11-01T16:59:59.999Z\\"}}","sort":"{\\"createdDate\\":-1}","skip":0,"limit":20,"search":""}"
        yield Request(
            url=self.url,
            method='POST',
            dont_filter=True,
            headers=self.headers,
            body=data,
            # meta= {
            #     "city_id": city_id,
            #     "seo_url": seo_url
            # },
            callback=self.parse_page
        )
    # with open ('ho-chi-minh.json','w') as f:
    #     f.write("[")
    def parse_page(self, response):
        data = json.loads(response.body)
        # total_ads = data["total"]
        # region = response.meta["region"]
        # city_id = response.meta["city_id"]
        # seo_url = response.meta["seo_url"]
        total_ads = 0
        if ('total' in data.keys()):
            if(int(data['total'])>0):
                total_ads = data['total']
            else:
                return
        number_page = round(total_ads/20)
        temp = "Ho Chi Minh"
        city_id = "5e5501caeb80a7245175de0c"
        seo_url = temp.lower().replace(" ","-")
        for page in range(1,number_page+1):
            data = '{"category":"5deb722db4367252525c1d00","filter":"{\\"attributes\\":{\\"5dfb2acdd5e511385e90df86\\":[\\"'+ city_id+'\\"]},\\"seoUrl\\":\\"'+seo_url+'\\",\\"date\\":{\\"startDate\\":\\"2001-01-19T17:00:00.000Z\\",\\"endDate\\":\\"2020-11-06T16:59:59.999Z\\"}}","sort":"{\\"createdDate\\":-1}","skip":'+str((page-1)*20)+',"limit":20,"search":""}'
            yield Request(
                url=self.url,
                method='POST',
                dont_filter=True,
                headers=self.headers,
                body=data,
                callback=self.parse_ads
            )

    def parse_ads(self,response):
        data = json.loads(response.body)
        yield {
            "data": data
        }


# ad_data ={'_id': data['_id'],
#           'updatedDate': data ['updatedDate'],
#           'metaDescription': data ['metaDescription'],
#           'area':data ['area'],
#           'creatorType': data['creatorType'],
#           'sortGeoloc': data['sortGeoloc'],
#           'geoloc': data['geoloc'],
#           'unit': data ['unit'],
#           'code': data ['code'],
#           'totalPrice': data['totalPrice'],
#           'media': data['media'],
#           'title':data['title'],
#           'startTime':data['startTime'],
#           'creator_phone': data['creator']['phone'],
#           'creator_name': data['creator']['name']['first']+ ' '+ data['creator']['name']['last'],
#           'creator_id': data['creator']['_id'],
#           'creator_email':data['creator']['email'],
#           'creator_type': data['creator']['type'],
#           'createdDate': data['createdDate'],
#           'location': data['location']['street']+ ', '+ data['location']['ward']+ ', '+data['location']['district']+ ', ' + data['location']['city'],
#           'endTime': data['endTime'],
#           'category_id': data['category']['_id'],
#           'category_slug':data['category']['slug']
          
        #  }
