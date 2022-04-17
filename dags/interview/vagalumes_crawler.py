
from urllib.request import urlopen
from bs4 import BeautifulSoup as Sp

url = "https://www.vagalume.com.br/top100/artistas/geral/2022/04/"
vagalumes_top100= urlopen(url)   
obj = Sp(vagalumes_top100.read(), 'html.parser')
musicas = []
for artist in obj.find_all("a", {"class" : "w1 h22"}, limit= 50): 
    artist_popularity = Sp(urlopen(f"https://www.vagalume.com.br/{artist.attrs['href']}/popularidade/"), 'html.parser')
    fatos_pops = artist_popularity.find(id="popFacts").find_all("a")
    musica_mais_acessada = fatos_pops[len(fatos_pops) -1].text 
    musicas.append(musica_mais_acessada)
print(musicas)
  

