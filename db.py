class DB_TABLE(object):

    def __repr__(self):
        return "\n".join(['{0}: {1}'.format(k, v) for k, v in self.__dict__.items()])


class DB(object):

    def __init__(self):
        self._db = {}

    def createTable(self, theTable, theFields, theCb=None):
        tb = DB_TABLE
        self._db.update({theTable: {'klass': tb, 'table': [], 'cb': theCb, 'id': 0}})
        for name, default in theFields.items():
            setattr(tb, name, default)
        return tb

    def addRowToTable(self, theTable, **kwargs):
        Tb = self._db[theTable]['klass']
        row = Tb()
        id = self._db[theTable]['id'] + 1
        setattr(row, 'id', id)
        self._db[theTable]['id'] = id
        for k, v in kwargs.items():
            setattr(row, k, v)
        self._db[theTable]['table'].append(row)
        if self._db[theTable]['cb']:
            self._db[theTable]['cb'](row)
        return row

    def deleteRowFromTable(self, theTable, theId):
        Tb = self._db[theTable]['table']
        for index, entry in enumerate(Tb):
            if entry.id == theId:
                break
        del Tb[index]

    def getRowFromTable(self, theTable, theId):
        Tb = self._db[theTable]['table']
        for index, entry in enumerate(Tb):
            if entry.id == theId:
                break
        return Tb[index]

    def getAllRowFromTable(self, theTable):
        Tb = self._db[theTable]['table']
        for entry in Tb:
            yield entry
