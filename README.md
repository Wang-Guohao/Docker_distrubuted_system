Started project by following this [blog post about creating a flask-mysql app with docker](https://stavshamir.github.io/python/dockerizing-a-flask-mysql-app-with-docker-compose/)

## Deployment and Usage:

Start the project by entering the project root through a terminal.

Use the command 'docker-compose up --build', and wait for it to be finished.

Now we can access the project at localhost/127.0.0.1 at port 5000

At root, we can see whether we are logged in. If not, there will be a button that redirects to /login.

There we can input one of the following three accounts and passwords:

```
user1 : 1111
user2 : 2222
user3 : 3333
```
After login, you can use different url in your broswer to achieve subscribe topic, get notify and pull.

You also can manke a topic advertise or not.

We have three broker nodes, you can visit each node by differen port in the host.

Broker1 is the main node, http://127.0.0.1:5000/

Broker2 is the main node, http://127.0.0.1:5002/

Broker3 is the main node, http://127.0.0.1:5003/


## Design:

The app is built using MySQL database using Python with a Flask framework. 

We use mysql-connector to let python speak to the mysql engine. 

HTTP requests (messages) managed by Flask which we can access by GET and POST requests through python in our app.py 

Docker is used to setup the environment in which we can compile and publish the project 

We used 8 container in this project, there are 3 broker nodes, 3 database, 1 publisher and 1 simulation client sub.

The structure of the whole system is following:

![DESIGN DIAGRAM](/Broker.jpg)

We only use the COMINGSOON API in IMDB as the external data resource.

Using those daily updated data, we generate 10 topics which ditributed in three brokers.

Broker1 manager the Content Rating topics

Broker2 manager the Movie Genre topics

Broker3 manager the Movie length topics

Publish():

	Implemented in /publisher 
	pub.py is where we push to the 3 endpoints in our central server.
	 
	The API request times is limited, so we use schedual to run this publisher everyday 23:54.
	You can change it in /publish/pub.py
	We provide some hisroty data in json format at /data document in the zip file which is optional when you needed.
```		
    http://app:5000/pub/comingmovie
    http://app:5000/pub/genre
    http://app:5000/pub/director
    http://app:5000/pub/star
```
Advertise():

	the publisher deseminates their data to the database through /pub/advertise
	
	You can set a topic advertise by the following url (need not login).
	http://127.0.0.1:5003/pub/advertise?topic=<topicid>

Deadvertise():

	the publisher sends a request to /pub/deadvertise which sets the data to be unseeable
	You can set a topic deadvertise by the following url (need not login).
	http://127.0.0.1:5003/pub/deadvertise?topic=<topicid>
Subscribe():
	We find a topic and save the user uid to it. That way if a movie with the topic appears, we can notify the subscribers. Goes through /sub/subscribe
	
	This interface need login. login account is mentioned above.

Unsubscribe():
	We find the associated topic and remove the user id from it through /sub/unsubscribe, the record will be removed from database.
	
	This interface need login. login account is mentioned above.

Notify():
	Notify can be represent by unread signitrue on the web service.
	
	This interface need login. login account is mentioned above.
	
Pull():
	User can pull the events list by this interface.
	
	When notify tells you have new event, invoke this interface can get unread events you subscribed.
	
	You can find the signal unread in this list.
	
	This interface need login. login account is mentioned above.
 
 UML diagram is following:
 ![UML DIAGRAM](/UML1.png)
