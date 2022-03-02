from logging import ERROR, INFO, info, log
from typing import List, Dict
from flask import Flask, render_template, flash
from flask import request, redirect, url_for, jsonify
import json
import flask_login
import requests
import dbconn
from user import User
from logging import Logger
import time
import mysql.connector

app = Flask(__name__, template_folder='templates/')
app.secret_key = 'superkey!@#123'
login_manager = flask_login.LoginManager()

login_manager.init_app(app)

log = Logger('test', ERROR)


# login user loader 
# used in login model
@login_manager.user_loader
def user_loader(id):
    return User.getById(id)

# login function
@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'GET':
        return render_template('login.html')
    if request.method == 'POST':
        username = request.form['username']
        password = request.form['password']
        print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++1",username)
        if username is None or password is None:
            print("unathorized handler")
            return unauthorized_handler()
        db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
        db.connect()
        tmp = db.find(sql="select * from user where username='%s'"%username)
        db.close()
        if tmp is None:
            print("incorrect password")
            return unauthorized_handler()
        if password == tmp['password']:
            print("password passed")
            user = User(tmp['id'], username, password, 1)
            user.set_authenticate(True)
            flask_login.login_user(user)
            # 登录成功返回页面
            print("still no error")
            return redirect(url_for('gohome'))
        else:
            return render_template('login.html')

# test login function , jump page
@app.route('/protected')
@flask_login.login_required
def protected():
    #a check to make sure users are logged in.
    return 'Logged in as: ' + flask_login.current_user.username
    #return redirect(url_for('gohome'))

# logout function
@app.route('/logout')
def logout():
    user = flask_login.current_user
    user.set_authenticate(False)
    #basic logout functionality that un-authenticates the user
    flask_login.logout_user()
    return redirect(url_for('gohome'))

# unauth handler
@login_manager.unauthorized_handler
def unauthorized_handler():
    #if invalid login credentials found, send back to login page
    time.sleep(2)
    return redirect(url_for('login'))


#search fundamental function to get a simple event
def find() -> List[Dict]:
    # time.sleep(1)
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    # connection = db.connect()
    #connection = mysql.connector.connect(**config)
    # cursor = connection.cursor()
    ## add filter in the future
    res = db.findAll(sql='SELECT tid, genre FROM datacenter.genre')
    results = [{tid: genre} for (tid, genre) in res]
    db.close()
    return results

#return a home page based on user authentication
@app.route('/', methods=['GET'])
def gohome() -> str:
    table = json.dumps({'???': find()})
    user = flask_login.current_user
    if user.is_authenticated:
        print("valid login")
        return render_template('home.html', test_tabledata1=table, authenticated=1, current_user=flask_login.current_user.username)
    else:
        print("invalid login detected")
        return render_template('home.html', test_tabledata1=table, authenticated=0)

@app.route('/index', methods=['GET'])
def goindex() -> str:
    return render_template('index.html', login_url=url_for('login'))

#how movie data is to be displayed, to be changed to template
@app.route('/movie_sample', methods=['GET'])
def check_sample() -> str:
    return render_template('movieblock.html')

@app.route('/movielist', methods=['GET'])
def get_movies() -> str:
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    movies = db.findAll(sql='SELECT * FROM datacenter.Movie')
    for movie in movies:
        tid = movie["id"]
        movie["genres"] = get_genreStr(db, tid)
        movie["directors"] = get_directorStr(db, tid)
        movie["stars"] = get_starStr(db, tid)
    #json.dumps(movies)
    return render_template('movieTemplate.html', movie_list=movies)


def get_genreStr(db, tid):
    genres = db.findAll('genre', "tid='%s'" % tid)
    output = ""
    for x in genres:
        output = output + x["genre"] + ", "
    output = output[:len(output)-2]
    return output

def get_directorStr(db, tid):
    directors = db.findAll('director', "tid='%s'" % tid)
    output = ""
    for x in directors:
        output = output + x["name"] + ", "
    output = output[:len(output) - 2]
    return output

def get_starStr(db, tid):
    stars = db.findAll('star', "tid='%s'" % tid)
    output = ""
    for x in stars:
        output = output + x["name"] + ", "
    output = output[:len(output) - 2]
    return output

# tmp interface test
@app.route('/get_genres', methods=['GET'])
def getgenres() -> str:
    if request.method == 'GET':
        db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
        db.connect()
        subscriptions = db.findAll(sql='SELECT * FROM genre')
        db.close()
        print(type(subscriptions))
        return {'sub': subscriptions}


# API for receiving the publisher sent data. Process data and  store in the database
@app.route('/pub/comingmovie', methods = ['POST'])
def receiveComingMovie() -> str:
    json = request.get_json()
    movie_data = json['data']
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    insert_list=[]
    for movie in movie_data:
        id = movie['id']
        mv = db.find('Movie',"id='%s'"%id)
        if not mv:
            db.insert('Movie', movie)
            insert_list.append(id)
    db.close()
    print('new ttid :')
    print(insert_list)
    print('---------------------------------------------')
    return {'list':insert_list}

# API for receiving publisher sent data. the data structure is tightly limited for the data body
@app.route('/pub/genre', methods = ['POST'], )
def receiveGenre() -> str:
    json = request.get_json()
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    for genre in json['data']:
        tid = genre['tid']
        for g in genre['genres']:
            params = {}
            params['tid'] = tid
            params['genre'] = g
            db.insert('genre', params)
    db.close()
    return {'msg':'success'}
# API for receiving publisher sent data. generate events whill processing
@app.route('/pub/director', methods = ['POST'])
def receiveDirector() -> str:
    json = request.get_json()
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    for director in json['data']:
        tid = director['tid']
        for d in director['directors']:
            d['tid'] = tid
            db.insert('director', d)
    db.close()
    return {'msg':'success'}

# accept the publisher send stars data here and generate some events of topic star.
@app.route('/pub/star', methods = ['POST'])
def receiveStar() -> str:
    json = request.get_json()
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    print(json)
    for star in json['data']:
        tid = star['tid']
        for s in star['stars']:
            #print(s)
            s['tid'] = tid
            db.insert('star', s)
    db.close()
    return {'msg':'success'}


@app.route('/subscribe_new', methods=['POST', 'GET'])
def subscribe_new():
    if request.method == 'GET':
        gr_dict = {'Horror': 0, 'Action': 0, 'Drama': 0}
        cr_dict = {'R': 0, 'PG-13': 0, '18': 0}
        ln_dict = {'<60 min': 0, '60 to 120 min': 0, '>120 min': 0}
        db = dbconn.Database(host='db', username='root', password='123456', database='datacenter')
        db.connect()
        userid = flask_login.current_user.id
        url_req = "http://localhost:5000/getsubs/cr/" + str(flask_login.current_user.id)
        subs1 = getRequest(url=url_req)
        print("subdump1: " + json.dumps(subs1))
        # url_req = "http://localhost:5000/getsubs/ln/"+ str(flask_login.current_user.id)
        # subs2 = getRequest(url=url_req)
        url_req = "http://localhost:5000/getsubs/gr/" + str(flask_login.current_user.id)
        subs3 = getRequest(url=url_req)
        if subs1:
            for x in subs1:
                cr_dict[x["name"][2:]] = 1
        # if subs2:
        # for x in subs2:
        #     ln_dict[x["name"][2:]] = 1
        if subs3:
            for x in subs3:
                gr_dict[x["name"][2:]] = 1
        db.close()
        return render_template('subscribe.html',
                               content_rating=cr_dict,
                               genres=gr_dict,
                               lengths=ln_dict)

@app.route('/subscribe_test', methods=['GET'])
@flask_login.login_required
def subscribe_example():
    if request.method == 'GET':
        genre_dict = {'Fantasy': 0, 'Action': 1, 'Romance': 1}
        cr_dict = {'R': 1, 'PG-13': 0, 'E': 1}
        db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
        db.connect()
        output = db.findAll(sql='SELECT DISTINCT genre FROM datacenter.genre')
        print("genre:"+json.dumps(output))
        genre_dict = {}
        for x in output:
            genre_dict[x["genre"]] = 0
        output = db.findAll(sql='SELECT DISTINCT contentRating FROM datacenter.Movie')
        print("cr: " + json.dumps(output))
        cr_dict = {}
        for x in output:
            cr_dict[x["contentRating"]] = 0
        output = db.findAll(sql='SELECT topicid, topic  FROM datacenter.topic WHERE topicid >= 7 AND topicid <= 9')
        ln_dict = {}
        for x in output:
            ln_dict[x["topic"]] = 0
        userid = flask_login.current_user.id
        output = db.findAll(table='subscription', where=f'userid = {userid}')
        print("subs: " + json.dumps(output))
        for x in output:
            type = x["name"][0:2]
            name = x["name"][2:]
            if(type == 'cr'):
                cr_dict[name] = 1
            if(type == 'gr'):
                genre_dict[name] = 1
            if(type == 'ln'):
                ln_dict[name] = 1
        db.close()
        return render_template('subscribe.html',
                               content_rating=cr_dict,
                               genres=genre_dict,
                               lengths=ln_dict)

@app.route('/pub/finish', methods = ['POST'])
def praseToEvents()-> str:
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    json = request.get_json()
    ids = json.get('ids')
    if not ids:
        return {'msg':'empty id list'}
    # process the Genre topic
    res = db.findAll(table='Movie', where=f'id in {tuple(ids)}')
    short_sub = db.findAll(table='subscription', where='topicid = 8')
    short_userids = [s['userid'] for s in short_sub]
    mid_sub = db.findAll(table='subscription', where='topicid = 9')
    mid_userids = [m['userid'] for m in mid_sub]
    long_sub = db.findAll(table='subscription', where='topicid = 10')
    long_userids = [l['userid'] for l in long_sub]
    all_user = db.findAll(table='user')
    user_ids = [u['id'] for u in all_user]
    for event in res:
        movie_length = event['runtimeMins']
        if movie_length < 60:
            new_id = db.insert('event', {'content': str(event), 'topicid': 8})
            t8 = db.find(table='topic',where='topicid=8')
            if t8['advertise'] == 1:
                for u in user_ids:
                    db.insert(table='user_event', params={'userid':u, 'eventid':new_id})
            else:
                for s in short_userids:
                    db.insert(table='user_event', params={'userid':s, 'eventid':new_id})
        elif movie_length >= 60 and movie_length < 120:
            new_id = db.insert('event', {'content': str(event), 'topicid': 9})
            t9 = db.find(table='topic',where='topicid=9')
            if t9['advertise'] == 1:
                for u in user_ids:
                    db.insert(table='user_event', params={'userid':u, 'eventid':new_id})
            else:
                for m in mid_userids:
                    db.insert(table='user_event', params={'userid':m, 'eventid':new_id})
        elif movie_length >= 120:
            new_id = db.insert('event', {'content': str(event), 'topicid': 10})
            t10 = db.find(table='topic',where='topicid=10')
            if t10['advertise'] == 1:
                for u in user_ids:
                    db.insert(table='user_event', params={'userid':u, 'eventid':new_id})
            else:
                for l in long_userids:
                    db.insert(table='user_event', params={'userid':l, 'eventid':new_id})
    db.close()
    return {'msg':'success'}


def gettopicID(db, topic):
    print("get topic id")
    tid = db.find('topic', "topic='%s'" % topic)
    print("tid: ", tid['topicid'])
    if not tid:
        max = db.find(sql="SELECT MAX(topicid) FROM topic;")
        print
        print(json.dumps(max))
        params = {}
        params['topicid'] = int(max["MAX(topicid)"]) + 1
        params['topic'] = topic
        params['advertise'] = 0
        db.insert('topic', params)
        return params['topicid']
    return tid['topicid']

# subscribe function for client, client can subscribe topic here
@app.route('/sub/subscribe/<type>/<topic>', methods = ['GET', 'POST'])
@flask_login.login_required
def subscribe(type, topic) -> str:
    userid = flask_login.current_user.id
    sub_list=[]
    local_res = localSubscribe(userid)
    broker2_res = getRequest(url=f'http://broker2:5000/sub/neighbour/subscribe', params={'userid':userid})
    app_res = getRequest(url=f'http://app:5000/sub/neighbour/subscribe', params={'userid':userid})
    if request.method == 'POST':
        #return topicid
        if type == 'ln':
            local_res = localSubscribe(userid,topic)
        elif type == 'cr':
            app_res = getRequest(url=f'http://app:5000/sub/neighbour/subscribe',params={'userid':userid,'topic':topic})
        elif type == 'gr':
            broker2_res = getRequest(url=f'http://broker2:5000/sub/neighbour/subscribe',params={'userid':userid,'topic':topic})
    # check
    if request.method == 'GET':
        print("sub/subscribed GET found! :(")
    sub_list.extend(local_res['sublist'])
    sub_list.extend(app_res.get('sublist'))
    sub_list.extend(broker2_res.get('sublist'))
    print(sub_list)
    return {'sublist':sub_list}

@app.route('/sub/neighbour/subscribe', methods = ['GET'])
def neighbourSub():
    print('+++++++++++++++++++++++++++++++++++')
    userid = request.args.get('userid')
    topic = request.args.get('topic')
    if topic is None:
        return localSubscribe(userid)
    else:
        return localSubscribe(userid, topic)

def localSubscribe(userid, topic=None):
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    if topic is None:
        subscriptions = db.findAll('subscription',"userid='%s'"%userid)
        db.close()
        return {'sublist': subscriptions}
    tid = db.find('topic', "topic='%s'" % topic)
    topicid = tid['topicid']
    subscriptions = db.findAll('subscription',"userid='%s'"%userid)
    for sub in subscriptions:
        if sub['topicid'] == topicid:
            db.close()
            return {'sublist': subscriptions}
    db.insert('subscription', {"userid": userid, "topicid": topicid, "name": ('ln'+'-'+topic)})
    subscriptions = db.findAll('subscription',"userid='%s'"%userid)
    db.close()
    return {'sublist': subscriptions}


# unsubscribe function for client, client can unsubscribe topic here
@app.route('/sub/unsubscribe/<type>/<topic>', methods = ['POST'])
@flask_login.login_required
def unsubscribe(type, topic) -> str:
    print("sub-subscribed found!")
    userid = flask_login.current_user.id
    sub_list=[]
    local_res = unsublocal(userid)
    app_res = getRequest(url=f'http://app:5000/sub/neighbour/unsubscribe',params={'userid':userid})
    broker2_res = getRequest(url=f'http://broker2:5000/sub/neighbour/unsubscribe', params={'userid':userid})
    if request.method == 'POST':
        #return topicid
        if type == 'ln':
            local_res = unsublocal(userid,topic)
        elif type == 'cr':
            app_res = getRequest(url=f'http://app:5000/sub/neighbour/unsubscribe',params={'userid':userid,'topic':topic})
        elif type =='gr':
            broker2_res = getRequest(url=f'http://broker2:5000/sub/neighbour/unsubscribe', params={'userid':userid,'topic':topic})
    # check
    if request.method == 'GET':
        print("/sub/unsubscribe GET found! :(")
    sub_list.extend(local_res['sublist'])
    sub_list.extend(app_res.get('sublist'))
    sub_list.extend(broker2_res.get('sublist'))
    print(sub_list)
    return {'sublist':sub_list}

@app.route('/sub/neighbour/unsubscribe', methods = ['GET'])
def neighbourUnsub():
    userid = request.args.get('userid')
    topic = request.args.get('topic')
    if topic is None:
        return unsublocal(userid)
    else:
        return unsublocal(userid, topic)

def unsublocal(userid, topic=None):
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    if topic is None:
        subscriptions = db.findAll('subscription',"userid='%s'"%userid)
        db.close()
        return {'sublist': subscriptions}
    tid = db.find('topic', "topic='%s'" % topic)
    topicid = tid['topicid']
    subscriptions = db.findAll('subscription',"userid='%s'"%userid)
    for sub in subscriptions:
        if sub['topicid'] == topicid:
            db.delete("subscription", "userid='%s' and topicid='%s'"%(userid, topicid))
            subscriptions = db.findAll('subscription',"userid='%s'"%userid)
            db.close()
            return {'sublist': subscriptions}
    db.close()
    return {'sublist': subscriptions}

@app.route('/get_submovie/<type>/<topic>', methods = ['POST'])
def get_submovie1():
    return request.json

@app.route('/get_submovie/cr/<topic>', methods=['GET'])
def send_crsubdata(topic):
    db = dbconn.Database(host='db', username='root', password='123456', database='datacenter')
    db.connect()
    output = db.findAll(table='Movie', where=f"contentRating = '{topic}'")
    db.close()
    return jsonify(output)

@app.route('/userchannel', methods = ['GET'])
@flask_login.login_required
def get_channels():
    if request.method == 'GET':
        db = dbconn.Database(host='db', username='root', password='123456', database='datacenter')
        db.connect()
        userid = flask_login.current_user.id

        movies = {}
        gr_mcodes = {}

        userid = flask_login.current_user.id
        url_req = "http://localhost:5000/getsubs/cr/" + str(userid)
        subs1 = getRequest(url=url_req)
        print("subdump1: " + json.dumps(subs1))
        # url_req = "http://localhost:5000/getsubs/ln/"+ str(userid)
        # subs2 = getRequest(url=url_req)
        url_req = "http://localhost:5000/getsubs/gr/" + str(userid)
        subs3 = getRequest(url=url_req)
        print("ucsubs: " + json.dumps(subs1))

        if subs1:
            for y in subs1:
                url_req = "http://localhost:5000/get_submovie/cr/" + y["name"][2:]
                output = getRequest(url=url_req)
                print("uc_movies: " + json.dumps(output))
                for x in output:
                    movies[x['id']] = x
        # if subs2:
        #     for y in subs2:
        #         url_req = "http://localhost:5000/get_submovie/ln/" + y["name"][2:]
        #         output = getRequest(url=url_req)
        #         for x in output:
        #             movies[x['id']] = x
        if subs3:
            for y in subs3:
                url_req = "http://localhost:5000/get_submovie/gr/" + y["name"][2:]
                output = getRequest(url=url_req)
                for x in output:
                    movies[x['id']] = x

        output = []
        for x in movies:
            output.append(movies[x])
        for movie in output:
            tid = movie["id"]
            movie["genres"] = get_genreStr(db, tid)
            movie["directors"] = get_directorStr(db, tid)
            movie["stars"] = get_starStr(db, tid)

        # json.dumps(movies)
        db.close()
        message = flask_login.current_user.username + "'s subscribed movies"
        return render_template('movieTemplate.html', movie_list=output, message=message)

# advertise function for publisher who what advertise their push.
# check
@app.route('/pub/advertise', methods = ['GET'])
def advertise() -> str:
    topicid = request.args.get('topic',type=int)
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    if topicid in (8,9,10):
        db.update(table='topic', where=f"topicid={topicid}", params={'advertise': '1'})
    elif topicid in (5,6,7):
        getRequest(url='http://app:5000/pub/advertise', params={'topic':topicid})
    elif topicid in (1,2,3,4):
        getRequest(url='http://broker2:5000/pub/advertise', params={'topic':topicid})   
    db.close()
    return 'success'

# unadvertise function for publisher to stop advertise
# check
@app.route('/pub/deadvertise', methods = ['GET'])
def deadvertise() -> str:
    topicid = request.args.get('topic',type=int)
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    if topicid in (8,9,10):
        db.update('topic', f"topicid={topicid}", {'advertise': '0'})
    elif topicid in (5,6,7):
        getRequest(url='http://app:5000/pub/deadvertise', params={'topic':topicid})
    elif topicid in (1,2,3,4):
        getRequest(url='http://broker2:5000/pub/deadvertise', params={'topic':topicid})   
    db.close()
    return 'success'

@app.route('/neighbour/notify', methods = ['GET'])
def neighbourNotify() -> bool:
    userid = request.args.get('userid')
    print(userid)
    return {'result':checkLocalUnread(userid)}

# Notify
@app.route('/notify', methods = ['GET'])
@flask_login.login_required
def notify() -> str:
    userid = flask_login.current_user.id
    # local status
    localTopicStatus = checkLocalUnread(userid)
    # app topic status
    broker1 = getRequest(url=f'http://app:5000/neighbour/notify',params={'userid':userid})
    # broker3 topic status
    broker2 = getRequest(url=f'http://broker2:5000/neighbour/notify',params={'userid':userid})
    if localTopicStatus or broker1.get('result') or broker2.get('result'):
        return 'new push received'
    else:
        return 'no new push'

def checkLocalUnread(userid):
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    res = db.find(sql=f'select sum(unread) as notify from datacenter.user_event where userid = {userid};')
    db.close()
    print(res)
    if res['notify'] is not None and res['notify'] > 0:
            return True
    else:
        return False


@app.route('/neighbour/pull', methods = ['GET'])
def neighbourPull():
    userid = request.args.get('userid')
    data = {}
    content_list = getLocalEventList(userid)
    data['data']=content_list
    return data

# Pull
@app.route('/pull', methods = ['GET'])
@flask_login.login_required
def getUserEventList() -> str:
    userid = flask_login.current_user.id
    data={}
    content_list = getLocalEventList(userid)
    # get remote brokers events
    broker1 = getRequest(f'http://app:5000/neighbour/pull', params={'userid':userid})
    broker2 = getRequest(f'http://broker2:5000/neighbour/pull', params={'userid':userid})
    b1_content_list = broker1.get('data')
    b2_content_list = broker2.get('data')
    content_list.extend(b1_content_list)
    content_list.extend(b2_content_list)
    output = []
    for x in content_list:
        output.append(json.loads(x['content']))
    message = flask_login.current_user.username + "'s subscribed movies"
    return render_template('movieTemplate.html', movie_list=output, message=message)

def getLocalEventList(userid)->list:
    db = dbconn.Database(host='db3', username='root', password='123456', database='datacenter')
    db.connect()
    event_list = db.findAll(table='user_event', where=f'userid="{userid}"')
    content_list = []
    for event in event_list:
        ueid = event['ueid']
        eid = event['eventid']
        unread = event['unread']
        res = db.find(table='event', where=f'eventid="{eid}"')
        content = res['content']
        content_list.append({'content':content,'unread':unread})
        db.update(table='user_event', where=f"ueid={ueid}", params={'unread': '0'})
    db.close()
    return content_list


# general get request, using to get data from external api
def getRequest(url, params=None):
    header = {'User-Agent': 'Mozilla'}
    try:
        r = requests.get(url, headers = header, params = params)
        r.raise_for_status()
        return r.json()
    except Exception:
        print("failed request ",Exception)

if __name__ == '__main__':
    app.run(host='0.0.0.0')
