from logging import Logger
import mysql.connector
import time

# abstract database connection and database operation
class Database:
    sql = []
    conn = None
    db = None
    # initial database config
    def __init__(self, host, username, password, database, port=3306, charset='utf8', tablePrefix='') -> None:
        self.host = host
        self.username = username
        self.password = password
        self.database = database
        self.port = int(port)
        self.charset = charset
        self.tablePrefix = tablePrefix
    # build up a connection
    def connect(self):
        if self.conn is None:
            try:
                time.sleep(1)
                self.conn = mysql.connector.connect(host=self.host, user=self.username, password=self.password, database=self.database)
                self.db = self.conn.cursor(dictionary = True)
                if self.charset is not None:
                    self.query("SET character_set_connection='%s', character_set_results='%s', character_set_client=binary"
                    %(self.charset,self.charset))
            except Exception as err:
                raise err
        return self.conn
    # count function
    def count(self, table='', where=None, sql=None):
        count = 0
        if not sql:
            where = where if where else '1=1'
            sql = "SELECT COUNT(1) AS NUM FROM %s WHERE %s" % (
                self.table(table), where)
        self.query(sql)
        rs = self.db.fetchone()
        if rs is not None:
            count = rs['NUM']
        return count
    # findOne function
    def find(self, table='', where=None, sql=None):
        if not sql:
            where = where if where else '1=1'
            sql = "SELECT * FROM %s WHERE %s limit 1" % (
                self.table(table), where)
        self.query(sql)
        return self.db.fetchone()
    # findAll function
    def findAll(self, table='', where=None, sql=None):
        if not sql:
            where = where if where else '1=1'
            sql = "SELECT * FROM %s WHERE %s" % (
                self.table(table), where)
        self.query(sql)
        return self.db.fetchall()

    # get data from a single column
    def findCol(self, table='', where=None, sql=None, col=None):
        col = col if col else 'id'
        if not sql:
            where = where if where else '1=1'
            sql = "SELECT COUNT(1) AS NUM FROM %s WHERE %s" % (
                self.table(table), where)
        self.query(sql)
        rs = self.db.fetchone()
        result = ''
        if rs is not None:
            result = rs[col]
        return result
    # general query with sql statement
    def query(self, sql, params=None):
        self.sql.append(sql)
        if params:
            t = type(params)
            if t == dict:
                return self.db.execute (sql, params)
            elif t== list:
                return self.db.executemany(sql, params)
        return self.db.execute(sql, params)
    # wraped insert function
    def insert(self, table, params):
        field = []
        t = type(params)
        if t == dict:
            field = params.keys()
        elif t == list:
            field = params[0]
            if not isinstance(field, dict):
                raise Exception("error params,need list[dict]")
            field = field.keys()
        else:
            raise Exception("error params,need dict")
        if not field:
            raise Exception("error params,need dict")
        values = '%({})s'.format(")s,%(".join(field))
        field = '`%s`' % "`,`".join(field)
        sql = "INSERT INTO %s (%s) VALUES (%s)" % (self.table(table), field, values)
        self.query(sql, params)
        self.conn.commit()
        return self.db.lastrowid
    # update statement
    def update(self, table, where, params):
        field = []
        t = type(params)
        if t == dict:
            field = list(params.keys())
        elif t == list:
            field = params[0]
            if not isinstance(field, dict):
                raise Exception("error params,need list[dict]")
            field = field.keys()
        else:
            raise Exception("error params,need dict")
        if not field or not isinstance(field, list):
            raise Exception("error params,need dict")
        t = []
        for i in field:
            t.append('`{}`=%({})s'.format(i,i))
        sql = "UPDATE %s SET %s WHERE %s" % (self.table(table), ",".join(t), where)
        self.query(sql, params)
        self.conn.commit()
        return True
    # delete function
    def delete(self, table, where):
        sql = "DELETE FROM %s WHERE %s"%(self.table(table), where)
        self.query(sql, None)
        self.conn.commit()
        return True
    # check sql log
    def sqlLog(self):
        return self.sql
    # close the database connection
    def close(self):
        self.db.close()
        self.conn.close()
    # prefix table name
    def table(self, tableName):
        return '%s%s' % (self.tablePrefix, tableName)
    
    def startStrans(self):
        self.conn.start_transaction()
    # commit modification
    def commit(self):
        self.conn.commit()
    # rollback
    def rollback(self):
        self.conn.rollback()

if __name__ == '__main__':
    db = Database(host='db2', username='root', password='123456', database='datacenter')
    time.sleep(20)
    db.connect()
    db.close()
