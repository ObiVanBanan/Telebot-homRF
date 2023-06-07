import os
import telebot
import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
import json
# from tqdm import tqdm 
import time
import logging

logging.basicConfig(level=logging.INFO, filename='bot_log.log', filemode='w',
                    format="%(asctime)s %(levelname)s %(message)s")

#Основной
bot = telebot.TeleBot('token')
URL_TEMPLATE = 'https://наш.дом.рф/сервисы/каталог-новостроек/список-объектов/список?objStatus=0&page=0&limit=10'
user_agent = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 OPR/97.0.0.0 (Edition Yx GX)'
}

def get_last_page():
    '''Получаеть последнюю страницу из каталога'''
    result = requests.get(url=URL_TEMPLATE, headers=user_agent)
    soup = bs(result.text, 'html.parser')
    ip = soup.find_all('a', class_='pagination-item')
    page_list = []
    for i in ip:
        page_list.append(i.text)
    return page_list[-1]

def get_data(page):
    '''Получает данные застройщика в формате JSON'''
    url = f'https://xn--80az8a.xn--d1aqf.xn--p1ai/%D1%81%D0%B5%D1%80%D0%B2%D0%B8%D1%81%D1%8B/api/kn/object?offset={page}0&limit=10&sortField=devId.devShortCleanNm&sortType=asc&objStatus=0'
    result = requests.get(url=url, headers=user_agent)
    return result

def cline(file):
    '''Преобразует таблицу'''
    list_fullName = []
    list_groupName = []
    df = pd.read_excel(file)

    #Из колонки developer берет fullName и groupName
    for n, name in enumerate(df['developer']):
        try:
            name = eval(name)
        except Exception as exc:
            pass

        try:
            list_fullName.append(name['fullName'])
        except Exception as exc:
            list_fullName.append('')
            pass
        try:
            list_groupName.append(name['groupName'])
        except Exception as exc:
            list_groupName.append('')
            pass
    #Из колонки objReady100PercDt берет дату и отзеркаливает ее
    for n, data in enumerate(df['objReady100PercDt']):
        year = data[:4]
        day = data[-2:]
        month = data[5:7]

        data = day + '.' + month + '.' + year
        df['objReady100PercDt'][n] = data

    #Создает 2 колонки fullName и groupName, заполняет данными
    df.insert(loc= 2 , column='fullName', value=list_fullName)
    df.insert(loc= 2 , column='groupName', value=list_groupName)

    #Удаляет колонки developer и Unnamed: 0
    df.drop(['developer', 'Unnamed: 0'], axis=1, inplace=True)

    #Переминовывает колонки
    df.rename(columns={
        'objId': 'ID',
        'rpdRegionCd': 'Регион',
        'objAddr': 'Адрес',
        'objFloorMax': 'Число этажей',
        'objReady100PercDt': 'Ввод в эксплуатацию',
        'objSquareLiving': 'Жилая площадь, м²',
        'fullName': 'Застройщик',
        'groupName': 'Группа компаний',
        'rpdNum': 'Проектная декларация',
                    }, inplace=True)

    #Сохраняет файл
    df.to_excel(file)

def main():
    millisec = time.time_ns()
    file = 'data' + str(millisec) + '.xlsx'
    #Получает число страниц
    last_page = get_last_page()

    #Создаем файл data.xlsx
    with open(file, 'w', encoding='utf-8') as f:
        f.close()

    #Создаем колонки
    data = pd.DataFrame(columns= [
                                'Unnamed: 0','objId','developer',
                                'rpdRegionCd','objAddr','objFloorMax',
                                'objReady100PercDt','objSquareLiving',
                                'rpdNum',
                                ])
    data.to_excel(file)
    df_1 = pd.read_excel(file)

    for page in range(0, int(last_page)):

        #Заходим на страницу и получаем данные от застройщиков в JSON формате
        ip = get_data(str(page))

        #Преобразуем JSON в список
        js = json.loads(ip.text)

        #Создаем датафрейм из словаря
        df_2 = pd.DataFrame(js['data']['list'])

        #Делаем общий датафрейм из df_1 и df_2
        df_1 = pd.concat([df_1, df_2], join='inner')

    df_1.to_excel(file)

    cline(file)

    return file

#Отрабатываем команду старт
@bot.message_handler(commands=["start"])
def bd_home(message):

    bot.send_message(message.from_user.id,'Взял в работу👨‍💻')
    bot.send_message(message.from_user.id,'Это может занять 5-10 минут⏳')

    try:
        logging.info('Взял в работу')
        file = main()

        #Отправляем документ
        bot.send_document(message.chat.id,open(file,"rb"))

        #Удаляем файл
        os.remove(file)
        logging.info('Отработал')
    except Exception as exc:
        bot.send_message(message.from_user.id,'Ошибка!❌')
        logging.error("Exception", exc_info=True)
        pass

@bot.message_handler(commands=["log_info"])
def log_info(message):

    bot.send_message(message.from_user.id,'Возвращаю логи👨‍💻')

    try:
        bot.send_document(message.chat.id,open('bot_log.log','rb'))
    except Exception as exc:
        print(exc)
        pass

# Запускаем бота
while True:
    try:
        bot.polling(none_stop=True, interval=0)
    except Exception as exc:
        logging.error("Exception", exc_info=True)
        time.sleep(15)