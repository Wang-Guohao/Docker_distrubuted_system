from logging import ERROR, Logger
# import flask_login
import dbconn


# User Object used in login model and represent every client

class User():
    __tablename__ = 'user'
    
    def __init__(self, id, username, password, status) -> None:
        self.id = str(id)
        self.username = username
        self.password = password
        self.status = status
        self.authenticated = False

    def is_active(self):
        """True, as all users are active."""
        return True

    def get_id(self):
        """Return the email address to satisfy Flask-Login's requirements."""
        return self.id

    def set_authenticate(self, b):
        self.authenticated = b

    def is_authenticated(self):
        """Return True if the user is authenticated."""
        return self.authenticated

    def is_anonymous(self):
        """False, as anonymous users aren't supported."""
        return False

    def getById(id):
        db = dbconn.Database(host='db', username='root', password='123456', database='datacenter')
        db.connect()
        tmp = db.find(sql="select * from user where id='%s'"%id)
        user = User(tmp['id'], tmp['username'], tmp['password'], 1)
        db.close()
        return user




# @login_manager.user_loader
# def user_loader(id):
#     return getById(id)



# @login_manager.request_loader
# def request_loader(request):
#     id = request.form.get('id')
#     user = getById(id)
#     return user