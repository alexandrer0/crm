from lxml import objectify
from lxml import etree
import pandas as pd
import sqlalchemy as sa
import config as cfg
from time import time

time_start = time()
# Подключение к БД
ora = sa.create_engine('postgresql+psycopg2://'+cfg.user_db+':'+cfg.pass_db+'@'+'localhost/'+cfg.db)
conn = ora.connect()

path = 'C:/develop/python/load_crm/RIO_BR.xml'
parser = etree.XMLParser(encoding='windows-1251', remove_comments=True)
xml = objectify.parse(open(path), parser=parser)
root = xml.getroot()
# print(root.tag)
# print(root.attrib)
target_date = root.attrib['begin-date']
table = []
tags = []
for z in root.getchildren():
    table.append(z.tag.replace('-', '_'))
    for x in z.getchildren():
        if x.attrib.keys() not in tags:
            tags.append(x.attrib.keys())
print(table)
print(tags)
for i in range(len(table)-1):
    attrib = []
    for a in root.getchildren():
        for b in a.getchildren():
            q = {**b.attrib}
            if a.tag.replace('-', '_') == table[i-1]:
                attrib.append(q)

    df = pd.DataFrame(attrib, columns=tags[i-1])
    df.rename(columns = lambda x: x.replace('-', '_'), inplace=True)
    df.insert(0, 'target_date', target_date)
    df['target_date'] = pd.to_datetime(df['target_date'], format='%Y%m%d')
    path_e = 'C:/develop/python/load_crm/rio_' + table[i-1] + '.xlsx'
    df.to_excel(path_e, index=False)
    df.to_sql(table[i-1], conn, 'mfo_br', if_exists='replace', index=False, chunksize=200)

print(round(time()-time_start,2), 'sec')
conn.close()