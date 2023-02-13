
import csv

import requests
from bs4 import BeautifulSoup
import time
from multiprocessing import cpu_count , Pool
from concurrent.futures import ThreadPoolExecutor





def fetch(url):

    links = []
    headings = []
    isMovies = ''
    https_proxy = '211.237.5.73:8898'
    proxylist = {
        "http"  : https_proxy, 
        "https" : https_proxy,
    }
    
    r = requests.get(url , proxies= proxylist)
    
    soup = BeautifulSoup(r.content, 'html5lib' ) 
    
    #print(url)
    
    a = soup.find('div' ,attrs = {"class":"thecontent clearfix"} )

    if a is not None:
        r = a.find('a' ,attrs = {"class":"maxbutton-1 maxbutton maxbutton-download-links"} )
        #print('Run')
        if r is not None:
            #print('Run')
            isMovies = '1'
            # for Movies
            
            for j in a.findAll('p',attrs={"style":"text-align: center"}):
                try:
                    links.append(j.a['href'])
                except:
                    continue
            heading1 = []
            for j in a.findAll('h4',attrs={"style":"text-align: center"}):
                heading1.append(j.text)

            headings = heading1
        else:
            #for episodes
            isMovies = '0'
            heading4 = []
            for j in a.findAll('h4',attrs={"style":"text-align: center"}):
                heading4.append(j.text)

            if len(heading4) == 0 :
                for j in a.findAll('h3',attrs={"style":"text-align: center"}):
                    heading4.append(j.text)
            headings = heading4

            
            for j in a.findAll('p',attrs={"style":"text-align: center"}):
                #print('Done')
                ro ={}
                for k in j.findAll('a'):
                    
                    try:
                        ro[k.span.text[1:] + '||'+ k['class'][2] ] = k['href']
                    except:
                        try:
                            ro[k.span.text[1:] + '||' ] = k['href']
                        except:
                            pass
                links.append(ro)
    if len(links)!=0:
        final = {}
        final['headings'] = headings
        final['links'] = links
        final['ismovies'] = isMovies
        final['paralink'] =  url[23:len(url)-1]
        print(final['paralink'])

        
        file_name =  final["paralink"] + ".txt"
        with open(file_name,'w', encoding="utf-8") as data: 
            data.write(str(final))
    print(".",end="" ,flush=True)
            

if __name__ == '__main__': 
    start_time = time.time()
    links = []
    with open('output.csv', newline='') as f:
        reader = csv.reader(f)
        for row in reader:
            if len(row) ==1 :
                links.append('https://moviesverse.me/' +row[0] + '/')
    
    print("Output file Read sucessfully ")
    # s = requests.Session()
    # for i in range(10 , 0  , -1):
    f= False
    if f:
        links = links[1]
        fetch(links)
    # with Pool(cpu_count()) as p:
    #      p.map(fetch , links)
    # Threading
    else:
        with ThreadPoolExecutor(max_workers=25) as executor:
            executor.map(fetch, links)
    
    end_time = time.time()
    print("Total Time " + str(end_time-start_time))

