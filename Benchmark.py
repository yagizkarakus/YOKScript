import requests
import time
import pandas as pd
import lxml
from lxml import html as LH
#from multiprocessing import cpu_count, Pool
from concurrent.futures import ThreadPoolExecutor
import openpyxl

def get_links(table, years, unicodes):
    links=[]
    for year in years:
        for unicode in unicodes:
            dyn_year = "" if year == 2022 else f"/{year}"
            link = f"https://yokatlas.yok.gov.tr{dyn_year}/content/lisans-dynamic/{table}.php?y={unicode}" 
            links.append(link)
    
    return links

def fetch(link):
    try:
            split = link.split("/")
            
            yil = "" if len(split) == 6 else "/"+split[3]
            nur = link.split("=")
            ur = f"https://yokatlas.yok.gov.tr{yil}/lisans.php?y={nur[1]}"
            
            res = []

            resp = requests.get(ur)
            
            page_contentd = LH.fromstring(resp.text)
            
            if page_contentd is not None:

                pghi = page_contentd.xpath(f"//div[@class='panel-heading']/h3")

                uni_adi = pghi[0].text
                fakulte = pghi[2].text
                
                pghi2 = page_contentd.xpath(f"//div[@class='panel-heading']/h2")

                bolum_adi = pghi2[0].text


                resp = requests.get(link)

                tgd = lxml.html.fromstring(resp.text)

                gg = tgd.xpath("table")

                '''d = pd.read_html(lxml.html.tostring(gg[0]), header=[0, 1, 2], skiprows=0)[0]'''

                d = pd.read_html(lxml.html.tostring(gg[0]), skiprows=0)[0]

                for idx, row in d.iterrows():

                    res.append(uni_adi.strip())

                    res.append(fakulte.strip())

                    res.append(bolum_adi.strip())

                    if idx == 0 or idx == 1 or idx == 2:

                        for col in range(0, 2):

                            res.append(row[col])

                        res.append("")

                    else:

                        for col in d.columns:

                            res.append(row[col])

                    res.append(yil)
                print(".",end="",flush=True)
                return res
    except:
        #print(link)
        pass


if __name__ == "__main__":
    unicodes = []
    df = pd.read_csv("unid.csv")
    for i in range(100):
        unicodes.append(df['department_id'][i])
    
    years = [2019, 2020, 2021, 2022]

    links = get_links("1080", years, unicodes)
    start_time = time.time()
    #session
    # s = requests.Session()
    # for link in links:
    #     fetch(link, s)
    #Multiprocessing
    with ThreadPoolExecutor (max_workers=70) as p:
        l = list(p.map(lambda url: fetch(url), links))

    print("\n\tTotal time taken", time.time()-start_time)
    print(l[-1])
    # data = {'universite': [], 'fakulte': [], 'bolum': [], 'Lise Tipi': [], 'yerlesen sayi': [], 'Oran': [], 'yil': []}
    # i = 0

    # while i < len(l):

    #     data['universite'].append(l[i])
    #     data['fakulte'].append(l[i + 1])

    #     data['bolum'].append(l[i + 2])

    #     data['Lise Tipi'].append(l[i + 3])

    #     data['yerlesen sayi'].append(l[i + 4])

    #     data['Oran'].append(l[i + 5])

    #     data['yil'].append(l[i + 6])

    #     i += 7
    #     df = pd.DataFrame(data, columns=data.keys())

        

    #    df.to_excel("output.xlsx")

    # For 2000 data
    # No optimization - 1293.3512768745422
    # Session - 671.9893219470978
    # Multiprocessing - 402.57015800476074
    # MultiThreading(50 thread) - 397.0804579257965
    # MultiThreading(70 thread) - 390.44946002960205