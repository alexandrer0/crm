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
tags = []
attribs = []
for z in root.getchildren():
    tags.append(z.tag)
    for x in z.getchildren():
        if x.attrib.keys() not in attribs:
            attribs.append(x.attrib.keys())

for i in range(len(tags)-1):
    attrib = []
    for a in root.getchildren():
        # print(a.tag)
        # print(a.attrib)
        for b in a.getchildren():
            # print(b.tag)
            # print(b.attrib)
            q = {**b.attrib}
            if a.tag == tags[i-1]:
                attrib.append(q)
    df = pd.DataFrame(attrib, columns=attribs[i-1])
    path_e = 'C:/develop/python/load_crm/rio' + tags[i-1] + '.xlsx'
    df.to_excel(path_e, index=False)

print(round(time()-time_start,2), 'sec')
conn.close()