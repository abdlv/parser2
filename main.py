import datetime
import time
import dateparser
import pymysql
import requests
from bs4 import BeautifulSoup

con = pymysql.connect(host='localhost',
                      user='root',
                      passwd='root',
                      db='news',
                      cursorclass=pymysql.cursors.DictCursor)

cursor = con.cursor()


def get_resource_instructions():
    query = 'SELECT * FROM resource'
    cursor.execute(query)
    data = cursor.fetchall()

    return data


def get_page_soup(url):
    response = requests.get(url, timeout=15)
    soup = BeautifulSoup(response.text, "html.parser")

    return soup


def get_urls(soup, base_url, top_tag):
    parts = top_tag.split('%')
    tag = parts[0]
    attribute = parts[1]
    value = parts[2]

    elements = soup.find_all(tag, {attribute: value})
    links = []

    for el in elements:
        if tag != 'a':
            link = el.find('a')
            if link is not None:
                link = link['href']
        else:
            link = el['href']

        if link is not None:
            link = base_url + link
            links.append(link)

    return links


def get_title(soup, title_cut):
    parts = title_cut.split('%')
    tag = parts[0]
    attribute = parts[1]
    value = parts[2]
    return soup.find(tag, {attribute: value}).text.strip()


def get_date(soup, date_cut):
    parts = date_cut.split('%')
    tag = parts[0]
    attribute = parts[1]
    value = parts[2]
    raw_date = soup.find(tag, {attribute: value}).text.strip()
    not_date = dateparser.parse(raw_date).strftime('%Y-%m-%d %H:%M:%S')
    return not_date


def get_text(soup, bottom_tag):
    parts = bottom_tag.split('%')
    tag = parts[0]
    attribute = parts[1]
    value = parts[2]
    return soup.find(tag, {attribute: value}).text.strip()


def insert_news(link, title, content, news_date):
    res_id = 0
    nd_date = datetime.datetime.strptime(news_date, '%Y-%m-%d %H:%M:%S').timestamp()
    s_date = str(time.time())
    not_date = news_date.split()[0]

    query = 'INSERT INTO items (res_id, link, title, content, nd_date, s_date, not_date)' \
            'VALUES (%s, %s, %s, %s, %s, %s, %s)'

    params = (res_id, link, title, content, nd_date, s_date, not_date)

    try:
        cursor.execute(query, params)
        con.commit()
    except pymysql.err.IntegrityError:
        pass


def get_data(url):
    headers = {
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br",
        "accept-language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        "cache-control": "max-age=0",
        "user - agent": "Mozilla / 5.0(Windows NT 10.0; Win64; x64) AppleWebKit / 537.36(KHTML, likeGecko) Chrome / 98.0.4758.109 Safari / 537.36OPR / 84.0.4316.52"
    }


def main():
    instructions = get_resource_instructions()
    for instruction in instructions:
        resource_id = instruction['RESOURCE_ID']
        resource_name = instruction['RESOURCE_NAME']
        resource_url = instruction['RESOURCE_URL']
        top_tag = instruction['top_tag']
        bottom_tag = instruction['bottom_tag']
        title_cut = instruction['title_cut']
        date_cut = instruction['date_cut']

        url_split = resource_url.split('$$$')
        url = url_split[0]
        try:
            base_url = url_split[1]
        except:
            base_url = ""

        soup = get_page_soup(url)
        news_urls = get_urls(soup, base_url, top_tag)

        args = []
        for news_url in news_urls:
            soup = get_page_soup(news_url)
            news_title = get_title(soup, title_cut)
            news_date = get_date(soup, date_cut)
            news_text = get_text(soup, bottom_tag)
            print('----------------')
            print('[TITLE] ' + news_title)
            print('[DATE] ' + str(news_date))
            print('[CONTENT] ' + news_text)
            print('----------------')
            print()
            args.append((news_url, news_title, news_text, news_date))
            insert_news(news_url, news_title, news_text, news_date)

        for ar in args:
            print(ar)


if __name__ == '__main__':
    main()
