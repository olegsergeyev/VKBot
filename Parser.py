import re
import requests as req
import json
import time
import datetime
import http.client
import sys
from contextlib import closing

import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.utils import get_random_id
from pymysql import cursors, connect


def patch_http_response_read(func):
    def inner(*args):
        try:
            return func(*args)
        except http.client.IncompleteRead as e:
            return e.partial
        except req.exceptions.ChunkedEncodingError as e:
            pass
    return inner

dom = 'https://www.instagram.com/p/'

def getlast(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT date FROM Links ORDER BY date DESC LIMIT 1")
        date = cursor.fetchone()
        if date is not None:
            date = date.get('date')
        else:
            date = datetime.fromtimestamp(0)
        return date

def parsemain(link):
    cont = 0
    with req.get(link) as r:
        cont = r
    text = cont.text
    result = re.findall(r'"shortcode":"\w+[-,_]*\w+"', text)
    for i in range(0, len(result)):
        result[i] = dom + result[i][13:-1]
    return result

def parselink(link):
    cont = 0
    with req.get(link) as r:
        cont = r
    text = cont.text
    result = re.findall(r'property="og:type" content="\w*.\w*"', text)

    try:
        if result[0][28:-1] == 'instapp:photo':
            link = re.findall(r'property="og:image" content=".*"', text)
            link = link[0][29:-1]
            desc = re.findall(r'property="og:description" content=".*"', text)
            desc = desc[0][35:-1]
            return link
    except IndexError:
        print('Wrong Index in ', link)
    time.sleep(1)


def imgdownload(url):
    cont = 0
    with req.get(url) as r:
        cont = r.content
    with open('file.jpg', 'wb') as localfile:
        localfile.write(cont)

def imgupload(link):
    url_server = vk.photos.getMessagesUploadServer(group_id="191133430")
    imgdownload(link)
    pho = open('file.jpg', 'rb')
    resp = 0
    with req.post(url_server.get('upload_url'), files={'photo': pho}) as r:
        resp = r.text
    resp = json.loads(resp)
    vk_link = vk.photos.saveMessagesPhoto(server=resp.get('server'), photo=resp.get('photo'), hash=resp.get('hash'))
    vk_link = 'photo' + str(vk_link[0].get('owner_id')) + '_' + str(vk_link[0].get('id'))
    return vk_link

def parsepage(connection , instagram_link):
    last_date = getlast(connection)
    while True:
        piclink = []
        links = parsemain(isntagramlink)
        for link in links:
            inslink = parselink(link)
            if inslink:
                piclink.append(inslink)
        with connection.cursor() as cursor:
            for ilink in piclink:
                query = "SELECT vklink FROM Links WHERE inslink=%s"
                cursor.execute(query, ilink)    
                vk_link = cursor.fetchone()
                if vk_link is not None:
                    vk_link = vk_link.get('vklink')
                else:
                    vk_link = imgupload(ilink)
                    query = "INSERT INTO Links (inslink, vklink) VALUES (%s, %s)"
                    cursor.execute(query, (ilink, vk_link))
                    connection.commit()
            query = "SELECT vklink, date FROM Links ORDER BY date DESC LIMIT 1"
            cursor.execute(query)
            cur = cursor.fetchone()
            pic = cur.get('vklink')
            cur_date = cur.get('date')
            print(cur_date, last_date, sep='  ')
            if cur_date > last_date:
                last_date = cur_date
                cursor.execute(f"SELECT user_id FROM Users")
                users = cursor.fetchall()
                users = [int(user.get('user_id')) for user in users]
                while users:
                    vk.messages.send(
                        user_ids=users[0:100],
                        random_id=get_random_id(),
                        attachment=pic,
                    )
                    users = users[100:]
        time.sleep(60)


if __name__ == '__main__':
    vk_session = vk_api.VkApi(token=sys.argv[1])
    vk = vk_session.get_api()
    longpool = VkBotLongPoll(vk_session, sys.argv[2])
    http.client.HTTPResponse.read = patch_http_response_read(http.client.HTTPResponse.read)
    with closing(connect(host='localhost',
                                user=sys.argv[3],
                                password=sys.argv[4],
                                db=sys.argv[5],
                                charset='utf8mb4',
                                cursorclass=cursors.DictCursor)) as connection:
                                isntagram_link = sys.argv[6]
                                parsepage(connection, instagram_link)
