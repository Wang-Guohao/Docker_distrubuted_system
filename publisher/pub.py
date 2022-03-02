from types import MethodType
import requests
from requests.api import post
import schedule
import time
import json
from logging import ERROR, Logger

log = Logger('debug', ERROR)

# general get request, using to get data from external api
def getRequest(url, params=None):
    # url = 'https://imdb-api.com/en/API/ComingSoon/k_3zohv33q'
    header = {'User-Agent': 'Mozilla'}
    try:
        r = requests.get(url, headers = header, params = params)
        r.raise_for_status()
        return r.json()
    except Exception:
        print("failed request ",Exception)

# general post request, using json format body carry the data
def postRequest(url, body):
    headers = {'Content-Type': 'application/json','user-agent': 'Mozilla'}
    try:
        r = requests.post(url, json=body, headers=headers)
        r.raise_for_status()
        return r.json()
    except Exception:
        log.error("failed request ", Exception)

# parsing json and return a generator
def parsingJSON(json):
    if json:
        items = json.get('items')
        for item in items:
            yield item

## Publisher
def publish1():
    url = 'https://imdb-api.com/en/API/ComingSoon/k_3zohv33q'
    res = getRequest(url=url)
    items = parsingJSON(res)
    ## the API has maxiunm invoke times, temperary use the json instead
    json = {"items": [{"id":"tt7740510","title":"Antlers","fullTitle":"Antlers (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BY2UzODAyNjktN2MwYy00M2RkLThiOTEtMjU1MTgxY2EzM2YyXkEyXkFqcGdeQXVyODk5MDA0MDU@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"99","runtimeStr":"1h 39mins","plot":"In an isolated Oregon town, a middle-school teacher and her sheriff brother become embroiled with her enigmatic student, whose dark secrets lead to terrifying encounters with a legendary ancestral creature who came before them.","contentRating":"R","imDbRating":"","imDbRatingCount":"","metacriticRating":"66","genres":"Drama, Horror, Mystery","genreList":[{"key":"Drama","value":"Drama"},{"key":"Horror","value":"Horror"},{"key":"Mystery","value":"Mystery"}],"directors":"Scott Cooper","directorList":[{"id":"nm0178376","name":"Scott Cooper"}],"stars":"Keri Russell, Jesse Plemons, Jeremy T. Thomas, Graham Greene","starList":[{"id":"nm0005392","name":"Keri Russell"},{"id":"nm0687146","name":"Jesse Plemons"},{"id":"nm8864596","name":"Jeremy T. Thomas"},{"id":"nm0001295","name":"Graham Greene"}]},
    {"id":"tt9639470","title":"Last Night in Soho","fullTitle":"Last Night in Soho (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BZjgwZDIwY2MtNGZlNy00NGRlLWFmNTgtOTBkZThjMDUwMGJhXkEyXkFqcGdeQXVyMTEyMjM2NDc2._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"116","runtimeStr":"1h 56mins","plot":"An aspiring fashion designer is mysteriously able to enter the 1960s where she encounters a dazzling wannabe singer. But the glamour is not all it appears to be and the dreams of the past start to crack and splinter into something darker.","contentRating":"R","imDbRating":"","imDbRatingCount":"","metacriticRating":"67","genres":"Drama, Horror, Mystery, Thriller","genreList":[{"key":"Drama","value":"Drama"},{"key":"Horror","value":"Horror"},{"key":"Mystery","value":"Mystery"},{"key":"Thriller","value":"Thriller"}],"directors":"Edgar Wright","directorList":[{"id":"nm0942367","name":"Edgar Wright"}],"stars":"Thomasin McKenzie, Anya Taylor-Joy, Diana Rigg, Michael Ajao","starList":[{"id":"nm5057169","name":"Thomasin McKenzie"},{"id":"nm5896355","name":"Anya Taylor-Joy"},{"id":"nm0001671","name":"Diana Rigg"},{"id":"nm3915767","name":"Michael Ajao"}]},
    {"id":"tt13544716","title":"My Hero Academia: World Heroes' Mission","fullTitle":"My Hero Academia: World Heroes' Mission (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BNTBhYjYzZjEtOTU0OC00N2Q3LTgzYzgtNTk2NDRmNzZhMjFmXkEyXkFqcGdeQXVyNTkyODc5MjA@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"104","runtimeStr":"1h 44mins","plot":"When a cult of terrorists ruins a city by releasing a toxin that causes people's abilities to spiral out of control, Japan's greatest heroes spread around the world in an attempt to track down the mastermind and put him to justice.","contentRating":"PG-13","imDbRating":"","imDbRatingCount":"","metacriticRating":"","genres":"Animation, Action, Adventure, Fantasy, Sci-Fi","genreList":[{"key":"Animation","value":"Animation"},{"key":"Action","value":"Action"},{"key":"Adventure","value":"Adventure"},{"key":"Fantasy","value":"Fantasy"},{"key":"Sci-Fi","value":"Sci-Fi"}],"directors":"Kenji Nagasaki","directorList":[{"id":"nm2568279","name":"Kenji Nagasaki"}],"stars":"Robbie Daymond, Tetsu Inada, Yûki Kaji, Ryan Colt Levy","starList":[{"id":"nm2837894","name":"Robbie Daymond"},{"id":"nm1132505","name":"Tetsu Inada"},{"id":"nm2569233","name":"Yûki Kaji"},{"id":"nm8816277","name":"Ryan Colt Levy"}]},
    {"id":"tt6992978","title":"The Souvenir: Part II","fullTitle":"The Souvenir: Part II (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BODllMjY0MWYtNTMzMi00MDU4LTllZjItY2ViMDkwNDRmZTI0XkEyXkFqcGdeQXVyMDA4NzMyOA@@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"106","runtimeStr":"1h 46mins","plot":"In the aftermath of her tumultuous relationship, Julie begins to untangle her fraught love for him in making her graduation film, sorting fact from his elaborately constructed fiction.","contentRating":"R","imDbRating":"","imDbRatingCount":"","metacriticRating":"98","genres":"Drama","genreList":[{"key":"Drama","value":"Drama"}],"directors":"Joanna Hogg","directorList":[{"id":"nm0389712","name":"Joanna Hogg"}],"stars":"Tilda Swinton, Honor Swinton Byrne, James Spencer Ashworth, Alice McMillan","starList":[{"id":"nm0842770","name":"Tilda Swinton"},{"id":"nm4944898","name":"Honor Swinton Byrne"},{"id":"nm10488416","name":"James Spencer Ashworth"},{"id":"nm8205905","name":"Alice McMillan"}]},
    {"id":"tt10925852","title":"A Mouthful of Air","fullTitle":"A Mouthful of Air (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BMTlmYjQwNjAtMTY0Yi00ZjJmLThmNTktZWUwYzNmYTQ3YjA2XkEyXkFqcGdeQXVyNjY1MTg4Mzc@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"105","runtimeStr":"1h 45mins","plot":"Julie Davis writes bestselling children's books about unlocking your fears, but has yet to unlock her own. When her daughter is born, that trauma is brought to the fore, and with it, a crushing battle to survive.","contentRating":"R","imDbRating":"","imDbRatingCount":"","metacriticRating":"","genres":"Drama","genreList":[{"key":"Drama","value":"Drama"}],"directors":"Amy Koppelman","directorList":[{"id":"nm6508110","name":"Amy Koppelman"}],"stars":"Amanda Seyfried, Britt Robertson, Jennifer Carpenter, Finn Wittrock","starList":[{"id":"nm1086543","name":"Amanda Seyfried"},{"id":"nm1429380","name":"Britt Robertson"},{"id":"nm1358539","name":"Jennifer Carpenter"},{"id":"nm1587729","name":"Finn Wittrock"}]},
    {"id":"tt9274670","title":"13 Minutes","fullTitle":"13 Minutes (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BZTBjNjdkNzItYjZjOC00MjhkLTk4ZTQtZTcyM2FmMGJlMDViXkEyXkFqcGdeQXVyMTEyMjM2NDc2._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"108","runtimeStr":"1h 48mins","plot":"Four families in a Heartland town are tested in a single day when a tornado hits, forcing paths to cross and redefining the meaning of survival.","contentRating":"PG-13","imDbRating":"","imDbRatingCount":"","metacriticRating":"","genres":"Action, Drama, Thriller","genreList":[{"key":"Action","value":"Action"},{"key":"Drama","value":"Drama"},{"key":"Thriller","value":"Thriller"}],"directors":"Lindsay Gossling","directorList":[{"id":"nm1761263","name":"Lindsay Gossling"}],"stars":"Thora Birch, Amy Smart, Anne Heche, Paz Vega","starList":[{"id":"nm0000301","name":"Thora Birch"},{"id":"nm0005442","name":"Amy Smart"},{"id":"nm0000162","name":"Anne Heche"},{"id":"nm0891895","name":"Paz Vega"}]},
    {"id":"tt15331462","title":"Planet Dune","fullTitle":"Planet Dune (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BYzVkNjZlMDctODEzMy00MWE1LWI4YTUtZjdmNDZhYmZhODI0XkEyXkFqcGdeQXVyNTkzMzg3NDM@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"","runtimeStr":"","plot":"A crew on a mission to rescue a marooned base on a desert planet turns deadly when the crew finds themselves hunted and attacked by the planet's apex predators: giant sand worms.","contentRating":"","imDbRating":"","imDbRatingCount":"","metacriticRating":"","genres":"Action, Adventure, Sci-Fi","genreList":[{"key":"Action","value":"Action"},{"key":"Adventure","value":"Adventure"},{"key":"Sci-Fi","value":"Sci-Fi"}],"directors":"Glenn Campbell, Tammy Klein","directorList":[{"id":"nm0132485","name":"Glenn Campbell"},{"id":"nm2090620","name":"Tammy Klein"}],"stars":"Sean Young, Emily Killian, Tammy Klein, Clark Moore","starList":[{"id":"nm0000707","name":"Sean Young"},{"id":"nm6827877","name":"Emily Killian"},{"id":"nm2090620","name":"Tammy Klein"},{"id":"nm2226083","name":"Clark Moore"}]}],
	"errorMessage": ""}
    #json2=[{"id":"tt7740510","title":"Antlers","fullTitle":"Antlers (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BY2UzODAyNjktN2MwYy00M2RkLThiOTEtMjU1MTgxY2EzM2YyXkEyXkFqcGdeQXVyODk5MDA0MDU@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"99","runtimeStr":"1h 39mins","plot":"In an isolated Oregon town, a middle-school teacher and her sheriff brother become embroiled with her enigmatic student, whose dark secrets lead to terrifying encounters with a legendary ancestral creature who came before them.","contentRating":"R","imDbRating":"","imDbRatingCount":"","metacriticRating":"66","genres":"Drama, Horror, Mystery","genreList":[{"key":"Drama","value":"Drama"},{"key":"Horror","value":"Horror"},{"key":"Mystery","value":"Mystery"}],"directors":"Scott Cooper","directorList":[{"id":"nm0178376","name":"Scott Cooper"}],"stars":"Keri Russell, Jesse Plemons, Jeremy T. Thomas, Graham Greene","starList":[{"id":"nm0005392","name":"Keri Russell"},{"id":"nm0687146","name":"Jesse Plemons"},{"id":"nm8864596","name":"Jeremy T. Thomas"},{"id":"nm0001295","name":"Graham Greene"}]},{"id":"tt9639470","title":"Last Night in Soho","fullTitle":"Last Night in Soho (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BZjgwZDIwY2MtNGZlNy00NGRlLWFmNTgtOTBkZThjMDUwMGJhXkEyXkFqcGdeQXVyMTEyMjM2NDc2._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"116","runtimeStr":"1h 56mins","plot":"An aspiring fashion designer is mysteriously able to enter the 1960s where she encounters a dazzling wannabe singer. But the glamour is not all it appears to be and the dreams of the past start to crack and splinter into something darker.","contentRating":"R","imDbRating":"","imDbRatingCount":"","metacriticRating":"67","genres":"Drama, Horror, Mystery, Thriller","genreList":[{"key":"Drama","value":"Drama"},{"key":"Horror","value":"Horror"},{"key":"Mystery","value":"Mystery"},{"key":"Thriller","value":"Thriller"}],"directors":"Edgar Wright","directorList":[{"id":"nm0942367","name":"Edgar Wright"}],"stars":"Thomasin McKenzie, Anya Taylor-Joy, Diana Rigg, Michael Ajao","starList":[{"id":"nm5057169","name":"Thomasin McKenzie"},{"id":"nm5896355","name":"Anya Taylor-Joy"},{"id":"nm0001671","name":"Diana Rigg"},{"id":"nm3915767","name":"Michael Ajao"}]},{"id":"tt13544716","title":"My Hero Academia: World Heroes' Mission","fullTitle":"My Hero Academia: World Heroes' Mission (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BNTBhYjYzZjEtOTU0OC00N2Q3LTgzYzgtNTk2NDRmNzZhMjFmXkEyXkFqcGdeQXVyNTkyODc5MjA@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"104","runtimeStr":"1h 44mins","plot":"When a cult of terrorists ruins a city by releasing a toxin that causes people's abilities to spiral out of control, Japan's greatest heroes spread around the world in an attempt to track down the mastermind and put him to justice.","contentRating":"PG-13","imDbRating":"","imDbRatingCount":"","metacriticRating":"","genres":"Animation, Action, Adventure, Fantasy, Sci-Fi","genreList":[{"key":"Animation","value":"Animation"},{"key":"Action","value":"Action"},{"key":"Adventure","value":"Adventure"},{"key":"Fantasy","value":"Fantasy"},{"key":"Sci-Fi","value":"Sci-Fi"}],"directors":"Kenji Nagasaki","directorList":[{"id":"nm2568279","name":"Kenji Nagasaki"}],"stars":"Robbie Daymond, Tetsu Inada, Yûki Kaji, Ryan Colt Levy","starList":[{"id":"nm2837894","name":"Robbie Daymond"},{"id":"nm1132505","name":"Tetsu Inada"},{"id":"nm2569233","name":"Yûki Kaji"},{"id":"nm8816277","name":"Ryan Colt Levy"}]},{"id":"tt6992978","title":"The Souvenir: Part II","fullTitle":"The Souvenir: Part II (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BODllMjY0MWYtNTMzMi00MDU4LTllZjItY2ViMDkwNDRmZTI0XkEyXkFqcGdeQXVyMDA4NzMyOA@@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"106","runtimeStr":"1h 46mins","plot":"In the aftermath of her tumultuous relationship, Julie begins to untangle her fraught love for him in making her graduation film, sorting fact from his elaborately constructed fiction.","contentRating":"R","imDbRating":"","imDbRatingCount":"","metacriticRating":"98","genres":"Drama","genreList":[{"key":"Drama","value":"Drama"}],"directors":"Joanna Hogg","directorList":[{"id":"nm0389712","name":"Joanna Hogg"}],"stars":"Tilda Swinton, Honor Swinton Byrne, James Spencer Ashworth, Alice McMillan","starList":[{"id":"nm0842770","name":"Tilda Swinton"},{"id":"nm4944898","name":"Honor Swinton Byrne"},{"id":"nm10488416","name":"James Spencer Ashworth"},{"id":"nm8205905","name":"Alice McMillan"}]},{"id":"tt10925852","title":"A Mouthful of Air","fullTitle":"A Mouthful of Air (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BMTlmYjQwNjAtMTY0Yi00ZjJmLThmNTktZWUwYzNmYTQ3YjA2XkEyXkFqcGdeQXVyNjY1MTg4Mzc@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"105","runtimeStr":"1h 45mins","plot":"Julie Davis writes bestselling children's books about unlocking your fears, but has yet to unlock her own. When her daughter is born, that trauma is brought to the fore, and with it, a crushing battle to survive.","contentRating":"R","imDbRating":"","imDbRatingCount":"","metacriticRating":"","genres":"Drama","genreList":[{"key":"Drama","value":"Drama"}],"directors":"Amy Koppelman","directorList":[{"id":"nm6508110","name":"Amy Koppelman"}],"stars":"Amanda Seyfried, Britt Robertson, Jennifer Carpenter, Finn Wittrock","starList":[{"id":"nm1086543","name":"Amanda Seyfried"},{"id":"nm1429380","name":"Britt Robertson"},{"id":"nm1358539","name":"Jennifer Carpenter"},{"id":"nm1587729","name":"Finn Wittrock"}]},{"id":"tt9274670","title":"13 Minutes","fullTitle":"13 Minutes (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BZTBjNjdkNzItYjZjOC00MjhkLTk4ZTQtZTcyM2FmMGJlMDViXkEyXkFqcGdeQXVyMTEyMjM2NDc2._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"108","runtimeStr":"1h 48mins","plot":"Four families in a Heartland town are tested in a single day when a tornado hits, forcing paths to cross and redefining the meaning of survival.","contentRating":"PG-13","imDbRating":"","imDbRatingCount":"","metacriticRating":"","genres":"Action, Drama, Thriller","genreList":[{"key":"Action","value":"Action"},{"key":"Drama","value":"Drama"},{"key":"Thriller","value":"Thriller"}],"directors":"Lindsay Gossling","directorList":[{"id":"nm1761263","name":"Lindsay Gossling"}],"stars":"Thora Birch, Amy Smart, Anne Heche, Paz Vega","starList":[{"id":"nm0000301","name":"Thora Birch"},{"id":"nm0005442","name":"Amy Smart"},{"id":"nm0000162","name":"Anne Heche"},{"id":"nm0891895","name":"Paz Vega"}]},{"id":"tt15331462","title":"Planet Dune","fullTitle":"Planet Dune (2021)","year":"2021","releaseState":"October 29","image":"https://m.media-amazon.com/images/M/MV5BYzVkNjZlMDctODEzMy00MWE1LWI4YTUtZjdmNDZhYmZhODI0XkEyXkFqcGdeQXVyNTkzMzg3NDM@._V1_UX128_CR0,4,128,176_AL_.jpg","runtimeMins":"","runtimeStr":"","plot":"A crew on a mission to rescue a marooned base on a desert planet turns deadly when the crew finds themselves hunted and attacked by the planet's apex predators: giant sand worms.","contentRating":"","imDbRating":"","imDbRatingCount":"","metacriticRating":"","genres":"Action, Adventure, Sci-Fi","genreList":[{"key":"Action","value":"Action"},{"key":"Adventure","value":"Adventure"},{"key":"Sci-Fi","value":"Sci-Fi"}],"directors":"Glenn Campbell, Tammy Klein","directorList":[{"id":"nm0132485","name":"Glenn Campbell"},{"id":"nm2090620","name":"Tammy Klein"}],"stars":"Sean Young, Emily Killian, Tammy Klein, Clark Moore","starList":[{"id":"nm0000707","name":"Sean Young"},{"id":"nm6827877","name":"Emily Killian"},{"id":"nm2090620","name":"Tammy Klein"},{"id":"nm2226083","name":"Clark Moore"}]}]
    # items = parsingJSON(json)
    params1, params2, params3, params4= {}, {}, {}, {}
    data_list=[]
    list1, list2, list3 = [], [], []
    # decompose the JSON String into dict, split the data into 4 tables in database
    for item in items:
        data = {}
        data['id'] = item.get('id')
        data['title'] = item.get('title')
        data['releaseState'] = item.get('releaseState')
        data['year'] = item.get('year')
        data['image'] = item.get('image')
        data['runtimeMins'] = 0 if item.get('runtimeMins')=='' else item.get('runtimeMins')
        data['plot'] = item.get('plot')
        data['contentRating'] = item.get('contentRating')
        data_list.append(data)
        tmp1, tmp2, tmp3= {}, {}, {}
        genre_list = []
        for genre in item.get('genreList'):
            genre_list.append(genre.get('key'))
        tmp1['tid'] = item.get('id')
        tmp1['genres'] = genre_list
        list1.append(tmp1)
        director_list = []
        for director in item.get('directorList'):
            director['nid'] = director.pop('id')
            director_list.append(director)
        tmp2['tid'] = item.get('id')
        tmp2['directors'] = director_list
        list2.append(tmp2)

        star_list = []
        for star in item.get('starList'):
            star['nid'] = star.pop('id')
            star_list.append(star)
        tmp3['tid'] = item.get('id')
        tmp3['stars'] = star_list
        list3.append(tmp3)
    params1['data'] = data_list
    params2['data'] = list1
    params3['data'] = list2
    params4['data'] = list3

    # using post request to send the preprocessed data to the Broker 
    # this container is linked with app container, using app represent address

    response1 = postRequest('http://app:5000/pub/comingmovie',params1)
    id_list = response1.get('list')
    if id_list:
        ids = [tid for tid in id_list]
        response2 = postRequest('http://app:5000/pub/genre',params2)
        response3 = postRequest('http://app:5000/pub/director',params3)
        response4 = postRequest('http://app:5000/pub/star',params4)
        response5 = postRequest(url='http://app:5000/pub/finish', body={'ids': ids})

    # The same api send same data to another broker to generate different topics
    res_broker2 = postRequest('http://broker2:5000/pub/comingmovie',params1)
    id_list = res_broker2.get('list')
    if id_list:
        ids = [tid for tid in id_list]
        response2 = postRequest('http://broker2:5000/pub/genre',params2)
        response3 = postRequest('http://broker2:5000/pub/director',params3)
        response4 = postRequest('http://broker2:5000/pub/star',params4)
        response5 = postRequest(url='http://broker2:5000/pub/finish', body={'ids': ids})
    res_broker3 = postRequest('http://broker3:5000/pub/comingmovie',params1)
    id_list = res_broker3.get('list')
    if id_list:
        ids = [tid for tid in id_list]
        response2 = postRequest('http://broker3:5000/pub/genre',params2)
        response3 = postRequest('http://broker3:5000/pub/director',params3)
        response4 = postRequest('http://broker3:5000/pub/star',params4)
        response5 = postRequest(url='http://broker3:5000/pub/finish', body={'ids': ids})
    
    return 'success'


# The API has limited times request, and data will not be change during few hours
# there are four small publisher in the publish1
#schedule.every().day.at("18:45").do(publish1)
#schedule.every(12).hour.do(publish2)
#schedule.every(8).hour.do(publish3)

def main():
    # in product env uncomment follow lines, function will periodically
    # request the API get newd ata
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
    #time.sleep(5)
    #publish1()
    # pass
    schedule.every().day.at("23:54").do(publish1)
    while True:
        schedule.run_pending()
        print('waiting................')
        print('main-start:', time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time())))
        time.sleep(30)

    
if __name__ == '__main__':
    main()



