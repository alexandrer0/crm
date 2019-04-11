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
col = ('fst-id', 'fst-code', 'fst-name', 'participant-id', 'price-zone-code',
       'unpriced-zone-num', 'vol-balance-flow', 'vol-balance-people')
fst = []

path = 'C:\develop\python\load_crm\RIO_BR.xml'
parser = etree.XMLParser(encoding='windows-1251', remove_comments=True)
xml = objectify.parse(open(path), parser=parser)
root = xml.getroot()
# print(root.tag)
# print(root.attrib)
for a in root.getchildren():
    print(a.tag)
    print(a.attrib)
    for b in a.getchildren():
        print(b.tag)
        print(b.attrib)
        if a.tag=='fst':
            q = {**b.attrib}
            fst.append(q)
        # print(data)

print(fst)
df = pd.DataFrame(fst, columns=col)
# print(df)
df.to_excel('C:\develop\python\load_crm\RIO_BR.xlsx', index=False)
print(round(time()-time_start,2), 'sec')
conn.close()