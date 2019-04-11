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

col_region = ('region-code', 'region-name')
col_area = ('energy-area-num', 'energy-area-name')
col_fuel = ('fuel-type', 'fuel-name')
col_part = ('participant-id', 'participant-code', 'participant-name')
col_dpg = ('dpg-id', 'dpg-code', 'dpg-name', 'dpg-type', 'is-impex', 'participant-id',
           'region-code', 'price-zone-code', 'unpriced-zone-num', 'is-disqualified', 'dpg-admission-date',
           'consumer2', 'energy-area-num', 'is-system', 'is-guarantee-supply-co', 'is-gaes',
           'is-aux-dpg', 'is-rd-specific-consumer', 'station-id', 'aux-dpgc-id', 'parent-dpgc-id',
           'forced-mode-type', 'is-preliminary-forced-mode', 'renewable-energy-type', 'section-number',
           'is-section-optimized', 'is-fsk', 'is-island-import-dpg', 'parallel-work-norm',
           'is-forem-trader', 'is-spot-trader', 'is-rp-rf-2098', 'section-code', 'section-code', 'exploitation-mode',
           'is-blocked', 'is-adjusting-object', 'is-regcon', 'fst-id', 'is-res', 'is-transit', 'installed-capacity',
           'vol-balance-people', 'vol-balance-con', 'vol-balance-flow', 'tariff-ee', 'is-outzone')
col_dgu = ('dgu-id', 'dgu-num', 'dgu-name', 'dpgg-id')
col_gu = ('gu-id', 'gu-code', 'gu-pnt-code', 'dgu-id', 'main-fuel-type-list',
          'installed-capacity', 'tariff-compel-mode-power')
col_station = ('station-id', 'station-code', 'station-name', 'participant-id', 'price-zone-code',
          'unpriced-zone-num', 'station-category', 'station-type', 'vol-balance-con')
col_fst = ('fst-id', 'fst-code', 'fst-name', 'participant-id', 'price-zone-code',
       'unpriced-zone-num', 'vol-balance-flow', 'vol-balance-people')

region = []
area = []
fuel = []
part = []
dpg = []
dgu = []
gu = []
station = []
fst = []

path = 'C:\develop\python\load_crm\RIO_BR.xml'
parser = etree.XMLParser(encoding='windows-1251', remove_comments=True)
xml = objectify.parse(open(path), parser=parser)
root = xml.getroot()
# print(root.tag)
# print(root.attrib)
zz = []
for z in root.getchildren():
    zz.append(z.tag)
print(zz)

for a in root.getchildren():
    print(a.tag)
    print(a.attrib)
    for b in a.getchildren():
        print(b.tag)
        print(b.attrib)
        q = {**b.attrib}
        if a.tag == 'region':
            region.append(q)
        elif a.tag == 'energy-area':
            area.append(q)
        elif a.tag == 'fuel':
            fuel.append(q)
        elif a.tag == 'participant':
            part.append(q)
        elif a.tag == 'dpg':
            dpg.append(q)
        elif a.tag == 'dgu':
            dgu.append(q)
        elif a.tag == 'gu':
            gu.append(q)
        elif a.tag == 'station':
            station.append(q)
        elif a.tag == 'fst':
            fst.append(q)

df_region = pd.DataFrame(region, columns=col_region)
df_area = pd.DataFrame(area, columns=col_area)
df_fuel = pd.DataFrame(fuel, columns=col_fuel)
df_part = pd.DataFrame(part, columns=col_part)
df_dpg = pd.DataFrame(dpg, columns=col_dpg)
df_dgu = pd.DataFrame(dgu, columns=col_dgu)
df_gu = pd.DataFrame(gu, columns=col_gu)
df_station = pd.DataFrame(station, columns=col_station)
df_fst = pd.DataFrame(fst, columns=col_fst)

# print(df)
df_region.to_excel('C:\develop\python\load_crm\RIO_region.xlsx', index=False)
df_area.to_excel('C:\develop\python\load_crm\RIO_area.xlsx', index=False)
df_fuel.to_excel('C:\develop\python\load_crm\RIO_fuel.xlsx', index=False)
df_part.to_excel('C:\develop\python\load_crm\RIO_part.xlsx', index=False)
df_dpg.to_excel('C:\develop\python\load_crm\RIO_dpg.xlsx', index=False)
df_dgu.to_excel('C:\develop\python\load_crm\RIO_dgu.xlsx', index=False)
df_gu.to_excel('C:\develop\python\load_crm\RIO_gu.xlsx', index=False)
df_station.to_excel('C:\develop\python\load_crm\RIO_station.xlsx', index=False)
df_fst.to_excel('C:\develop\python\load_crm\RIO_fst.xlsx', index=False)
print(round(time()-time_start,2), 'sec')
conn.close()