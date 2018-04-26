import requests
from config import *
from lxml import etree
import json
import pymongo
from multiprocessing import Process,Pool
from gevent import monkey;monkey.patch_all()

class Comment:
    def __init__(self):
        self.headers = HEADERS
        self.client = pymongo.MongoClient(MONGO_URI)
        self.db = self.client[MONGO_DB]


    def start_urls(self):
        start_urls = START_URLS
        for url in start_urls:
            for x in range(65,91):
                response = requests.get(url.format(initial=x),headers=self.headers)
                yield response


    def parse_start_urls(self,responses): #解析start_urls获得artist_id,并生成歌手专辑的url的response
        for response in responses:
            html = etree.HTML(response.text)
            artists = html.xpath('//ul[@id="m-artist-box"]/li')
            for artist in artists:
                artist_name = artist.xpath('.//a[@class="nm nm-icn f-thide s-fc0"]/text()')[0]
                artist_id = artist.xpath('.//a[@class="nm nm-icn f-thide s-fc0"]/@href')[0][11:].replace('=','')
                params = {'id':artist_id,'limit':2000}
                response = requests.get('http://music.163.com/artist/album?',params=params,headers=self.headers)
                yield response



    def parse_album_urls(self,responses): #解析album_urls获得album_id,并生成专辑内歌曲页面的response
        for response in responses:
            html = etree.HTML(response.text)
            albums_href = html.xpath('//*[@id="m-song-module"]/li//a[@class="msk"]/@href')
            for album_href in albums_href:
                album_id = album_href.replace('/album?id=','')
                params = {'id':album_id}
                response = requests.get('http://music.163.com/album?',params=params,headers=self.headers)
                yield response


    def parse_song_urls(self,responses): #解析song_urls获得song_id,并生成非加密的歌曲评论（前20条评论）的response（格式为json）
        for response in responses:
            html = etree.HTML(response.text)
            songs = html.xpath('//ul[@class="f-hide"]/li')
            for song in songs:
                song_id = song.xpath('./a/@href')[0].replace('/song?id=','')
                response = requests.get('http://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}'.format(song_id=song_id),headers=self.headers)
                yield response



    def get_comment_urls(self,responses): #解析json并生成所以评论的urls
        for response in responses:
            song_id = response.url.replace('http://music.163.com/api/v1/resource/comments/R_SO_4_','')
            text = response.text
            result = json.loads(text)
            total = result['total']
            integer = int(total) // 100
            for x in range(integer + 1):
                response = requests.get('http://music.163.com/api/v1/resource/comments/R_SO_4_{song_id}?limit=100&offset={offset}'.format(song_id=song_id,offset=x*100),headers=self.headers)
                yield response


    def get_comment(self,responses):
        for response in responses:
            text = response.text
            result = json.loads(text)
            comments = result['comments']
            for comment in comments:
                comment = comment['content']
                result = {
                    'comment':comment
                }
                self.save_to_mongo(result)

    def save_to_mongo(self,result):
        if self.db['comment'].insert(result):
            print('存储成功',result)
            return True
        else:
            return False




    def start(self):
        response = self.start_urls()
        response = self.parse_start_urls(response)
        response = self.parse_album_urls(response)
        response = self.parse_song_urls(response)
        response = self.get_comment_urls(response)
        self.get_comment(response)



if __name__ == '__main__':
    c = Comment()
    c.start()


