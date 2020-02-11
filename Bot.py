import json
import sys
from contextlib import closing

from pymysql import cursors, connect
from vk_api import VkApi
from vk_api.utils import get_random_id
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType

keyboard = open('keyboard.json', 'rb').read()

def insert_user_id(user_id):
    with connection.cursor() as cursor:
        query = f"INSERT IGNORE INTO Users (user_id) VALUES ({user_id})"
        cursor.execute(query)
        connection.commit()

def delete_user_id(user_id):
    with connection.cursor() as cursor:
        query = f"DELETE FROM Users Where user_id = {user_id}"
        cursor.execute(query)
        connection.commit()

def send_answer(user_id, user_id_from_db, text_from_user):
    if text_from_user == 'ДА':
        insert_user_id(user_id)
        vk.messages.send(
            user_id=user_id,
            random_id=get_random_id(),
            message="Вы подписаны на обновления! Чтобы отписаться отправьте \"СТОП\""
        )
    elif text_from_user == 'СТОП':
        delete_user_id(user_id)
        vk.messages.send(
            user_id=user_id,
            random_id=get_random_id(),
            message="Вы отписаны от обновлений!"
        )
    elif text_from_user != '':
        if not user_id_from_db:
            vk.messages.send(
                user_id=user_id,
                random_id=get_random_id(),
                message="Отправьте \"ДА\" чтобы подписаться на обновления",
                keyboard=keyboard
            )
        else:
            vk.messages.send(
                user_id=user_id,
                random_id=get_random_id(),
                message="Чтобы отписаться отправьте \"СТОП\"",
                keyboard=keyboard
            )

def main(connection):
    events = longpool.listen()
    for event in events:
        if not event.type == VkBotEventType.MESSAGE_NEW:
            print()
            continue
        if not event.obj.get('message'):
            print()
            continue
        if not event.from_user:
            print()
            continue
        message = event.obj.get('message')
        user_id = message.get('from_id')
        text_from_user = message.get('text')
        user_id_from_db = 0
        try:
            with connection.cursor() as cursor:
                user_id_from_db = int(cursor.execute(f"SELECT user_id from Users where user_id={user_id}"))
        except Exception as e: 
            print(e)
            continue
        send_answer(user_id, user_id_from_db, text_from_user)
        


if __name__ == '__main__':    
    vk_session = VkApi(token=sys.argv[1])
    vk = vk_session.get_api()
    longpool = VkBotLongPoll(vk_session, sys.argv[2])
    with closing(connect(host='localhost',  
                         user=sys.argv[3],  # в парамсы
                         password=sys.argv[4],# в парамсы
                         db=sys.argv[5],# в парамсы
                         charset='utf8mb4',
                         cursorclass=cursors.DictCursor)) as connection:
        main(connection)
