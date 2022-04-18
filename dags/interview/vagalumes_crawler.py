
from urllib.request import urlopen 
from bs4 import BeautifulSoup as Sp

import aiohttp
import asyncio
import time


def vagalumes_top100_crawler(url): 
    page = urlopen(url) 
    bs4 = Sp(page.read(), 'html.parser')  
    base_link = "https://www.vagalume.com.br"

    urls = []
    for tag in bs4.find_all("a", {"class" : "w1 h22"}, limit= 100):  
        urls.append(base_link + tag.attrs["href"] + "popularidade/") 
    return urls

# ASYNC CODE
async def url_open(url, session):
    async with session.get(url) as response: 
        return await response.read()

async def request_popularity_data(urls):
    async with aiohttp.ClientSession() as session: 
        tasks = [] 
        for url in urls: 
            tasks.append(asyncio.create_task(url_open(url, session)))
        return await asyncio.gather(*tasks)  

def get_musicas(data): 
    artist_popularity = Sp(data, 'html.parser') 
    fatos_pops = artist_popularity.find(id="popFacts").find_all("a")
    musica_mais_acessada = fatos_pops[len(fatos_pops) -1].text 
    return musica_mais_acessada 

def main(urls): 
    data =  asyncio.run(request_popularity_data(urls))
    musics = []
    for d in data: 
        musics.append(get_musicas(d)) 
    return musics




url = "https://www.vagalume.com.br/top100/artistas/geral/2022/04/"
urls = vagalumes_top100_crawler(url)
start_time = time.time()
print(main(urls))
print("--- %s seconds ---" % (time.time() - start_time))

# SYNC CODE
def vagalumes_popularidade_scrapper(urls): 
    musicas = []
    for url in urls: 
        artist_popularity = Sp(urlopen(url).read(), 'html.parser')
        fatos_pops = artist_popularity.find(id="popFacts").find_all("a")
        musica_mais_acessada = fatos_pops[len(fatos_pops) -1].text 
        musicas.append(musica_mais_acessada)
    return musicas
start_time = time.time()
print(vagalumes_popularidade_scrapper(urls)) 
print("--- %s seconds ---" % (time.time() - start_time))