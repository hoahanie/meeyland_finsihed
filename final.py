import scrapy
import urllib
import json
import datetime
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy import Request
from datetime import date
import csv
import os.path


class muabanSpider(scrapy.Spider):
    name = 'new'
    base_url = 'https://api.meeyland.com/api/search'

    custom_settings = {
        'DOWNLOAD_DELAY': 0.5,
    }

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
        'referer': 'https://meeyland.com/mua-ban-nha-dat',
        'accept-language': 'en-US,en;q=0.9,vi;q=0.8'
        }

    def __init__(self):
        self.tempTime = ''
        self.startDate = '2001-02-19'
        self.endDate= str(date.today())

    def start_requests(self):
        para ={
            "category":"5deb722db4367252525c1d00",
            "filter":"{\"attributes\":{},\"seoUrl\":\"\",\"date\":{\"startDate\":\""+self.startDate +  "T00:00:00.000Z\",\"endDate\":\"" + self.endDate +"T00:00:00.000Z\"}}",
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
            callback=self.parse_ads
            )
                # break

    def parse_ads(self, response):
        data = json.loads(response.body)
        total_ads = data["total"]
        number_page = round(total_ads/20)
        number_page = 20
        for page in range(1,number_page+1):
            para ={
                "category":"5deb722db4367252525c1d00",
                "filter":"{\"attributes\":{},\"seoUrl\":\"\",\"date\":{\"startDate\":\""+self.startDate +  "T00:00:00.000Z\",\"endDate\":\"" + self.endDate +"T00:00:00.000Z\"}}",
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
                callback=self.parse_page
                )

    def parse_page(self,response):
        data = json.loads(response.body)
        ad_data ={'_id': '',
                    'updatedDate': '',
                    'metaDescription': '',
                    'area':'',
                    'creatorType': '',
                    'sortGeoloc': '',
                    'geoloc': '',
                    "unitPrice":'',
                    'totalPrice': '',
                    'media': '',
                    'title':'',
                    'startTime':'',
                    'creator_phone': '',
                    'creator_name': '',
                    'creator_id': '',
                    'creator_email':'',
                    'creator_type': '',
                    'createdDate': '',
                    'location': '',
                    'endTime': '',
                    'category_id': '',
                    'slug':'',
                    "5dfb2acdd5e511385e90df86": '',
                    "5dfb2af5d5e511385e90df91": '',
                    "5dfb2b34d5e511385e90df9c": '',
                    '5e4f47f1d86a7b3d53d59ae7': '',
                    '5dfa5e0359a281c7221c2335': '',
                    '5df6630bba09ec22616c3532': '',
                    '5dfb7072d5e511385e90e01c': '',
                    '5df65f90eb02bf5dff0fbffe': '',
                    '5df66112eb02bf5dff0fc009': '',
                    '5dfa597659a281c7221c2324': '',
                    '5dfa5f3e59a281c7221c2340': '',
                    '5dfa723059a281c7221c23b3': '',
                    '5dfa741659a281c7221c23c4': '',
                    '5dfa74d559a281c7221c23d4': '',
                    '5df66615eb4f4d34f9c48191': '',
                    '5dfa788e59a281c7221c2405': '',
                    '5dfa790459a281c7221c2410': '',
                    '5e4f8282eac2cf6ac3432b38': '',
                    '5e5020a2d4f8a9c5471e12d2': '',
                    '5e502100d4f8a918891e12d3': '',
                    '5e502133d4f8a977781e12d4': '',
                    '5e50dbbc7fb8300f47140100': '',
                    '5e502177d4f8a9528b1e12d6': '',
                    '5e50dc037fb8301180140102': '',
                    '5e50dd20f0543b8a805b27f5': '',
                    '5e50dcb9f0543bfcdb5b27f4': '',
                    '5dfa75db59a281c7221c23f4': '',
                    '5e1ebefd2fed7cb3fd6c010d': '',
                    '5df65f69eb02bf5dff0fbff3': '',
                    }
        for item in data["articles"]:
            ad_data['_id']= item['_id']
            try:
                ad_data['updatedDate']= item ['updatedDate']
            except:
                ad_data['updatedDate']='null'

            ad_data['metaDescription']= item ['metaDescription']
            ad_data['area']=item ['area']
            ad_data['creatorType']= item['creatorType']
            try:
                ad_data['sortGeoloc']= item['sortGeoloc']
            except:
                pass
            try:
                ad_data['geoloc']= item['geoloc']
            except:
                pass
            ad_data["unitPrice"]=item["unitPrice"]
            ad_data['totalPrice']= item['totalPrice']
            ad_data['media']= str(item['media'])
            ad_data['title']=item['title']
            ad_data['startTime']=item['startTime']
            ad_data['creator_phone']= item['creator']['phone']
            ad_data['creator_name']= str(item['creator']['name']['first'])+ ' '+ str(item['creator']['name']['last'])
            ad_data['creator_id']= item['creator']['_id']
            ad_data['creator_email']=item['creator']['email']
            ad_data['creator_type']= item['creator']['type']
            ad_data['createdDate']= item['createdDate']
            ad_data['location']= str(item['location']['street'])+ ', '+ str(item['location']['ward'])+ ', '+str(item['location']['district'])+ ', ' + str(item['location']['city'])
            ad_data['endTime']= item['endTime']
            ad_data['category_id']= item['category']['_id']
            ad_data['slug']=item['slug']

            for i in  item['attributes']:
                if i['id'] in list(ad_data.keys()):
                    try:
                        if i['id'] != "5dfa75db59a281c7221c23f4": 
                            ad_data[i['id']]= i['values'][0]['value']
                        else:
                            try:
                                a = ''
                                for e in i['values']:
                                    a += e['value'] + ', '
                                ad_data[i['id']] = a
                            except:
                                ad_data[i['id']]= i['values'][0]['value']
                        try:
                            if i['id'] == '5dfb7072d5e511385e90e01c':
                                ad_data[i['id']]= i['values'][0]['label']
                        except:
                            ad_data[i['id']]= 'null'

                    except:
                        ad_data[i['id']]=''  
            item_out = {
                '_id': ad_data['_id'],
                'updatedDate': ad_data['updatedDate'],
                'metaDescription': ad_data['metaDescription'],
                'area':ad_data['area'],
                'creatorType': ad_data['creatorType'],
                'sortGeoloc': ad_data['sortGeoloc'],
                'geoloc': ad_data['geoloc'],
                "unitPrice":ad_data["unitPrice"],
                'totalPrice': ad_data['totalPrice'],
                'media': ad_data['media'],
                'title':ad_data['title'],
                'startTime':ad_data['startTime'],
                'creator_phone': ad_data['creator_phone'],
                'creator_name': ad_data['creator_name'],
                'creator_id': ad_data['creator_id'],
                'creator_email':ad_data['creator_email'],
                'creator_type': ad_data['creator_type'],
                'createdDate': ad_data['createdDate'],
                'location': ad_data['location'],
                'endTime': ad_data['endTime'],
                'category_id': ad_data['category_id'],
                'slug':ad_data['slug'],
                "tinh_thanhpho": ad_data["5dfb2acdd5e511385e90df86"],
                "quan_huyen": ad_data["5dfb2af5d5e511385e90df91"],
                "xa_phuong": ad_data["5dfb2b34d5e511385e90df9c"],
                'duong': ad_data['5e1ebefd2fed7cb3fd6c010d'],
                'nhucau': ad_data['5e4f47f1d86a7b3d53d59ae7'],
                'loainhadat': ad_data['5dfa5e0359a281c7221c2335'],
                'loaihinh': ad_data['5df6630bba09ec22616c3532'],
                'duan': ad_data['5dfb7072d5e511385e90e01c'],
                'mattien(m)': ad_data['5df65f90eb02bf5dff0fbffe'],
                'dientich': ad_data['5df66112eb02bf5dff0fc009'],
                'huong': ad_data['5df66615eb4f4d34f9c48191'],
                'dacdiem_noibat': ad_data['5dfa75db59a281c7221c23f4'],
                'chieusau': ad_data['5df65f69eb02bf5dff0fbff3'],
                'duongrong': ad_data['5dfa597659a281c7221c2324'],
                'sotang': ad_data['5dfa5f3e59a281c7221c2340'],
                'sophongngu': ad_data['5dfa723059a281c7221c23b3'],
                'giaytophaply': ad_data['5dfa741659a281c7221c23c4'],
                'mucdogiaodich': ad_data['5dfa74d559a281c7221c23d4'],
                'hoahongmoigioi': ad_data['5dfa788e59a281c7221c2405'],
                'tienhoahong': ad_data['5dfa790459a281c7221c2410'],
                'tentaikhoan': ad_data['5e4f8282eac2cf6ac3432b38'],
                'email': ad_data['5e5020a2d4f8a9c5471e12d2'],
                'sodienthoai': ad_data['5e502100d4f8a918891e12d3'],
                'ten_facebook': ad_data['5e50dd20f0543b8a805b27f5'],
                'sdt_zalo': ad_data['5e50dcb9f0543bfcdb5b27f4'],
                '5e502133d4f8a977781e12d4': ad_data['5e502133d4f8a977781e12d4'],
                '5e50dbbc7fb8300f47140100': ad_data['5e50dbbc7fb8300f47140100'],
                '5e502177d4f8a9528b1e12d6': ad_data['5e502177d4f8a9528b1e12d6'],
                '5e50dc037fb8301180140102': ad_data['5e50dc037fb8301180140102'],
            }
            csv_file_name = "Names3.csv"
            csv_columns =  list(item_out.keys())
            
            header = {}
            for i in csv_columns:
                header[i] = i

            if os.path.exists(csv_file_name):
                with open(csv_file_name, 'a', encoding='utf-8') as csv_file:  
                    writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
                    writer.writerow(item_out)
            else:
                with open(csv_file_name, 'a', encoding='utf-8') as csv_file:  
                    writer = csv.DictWriter(csv_file, fieldnames=csv_columns)
                    writer.writerow(header)
                    writer.writerow(item_out)


if __name__ == '__main__':
    # run scraper
    process = CrawlerProcess()
    process.crawl(muabanSpider)
    process.start()

