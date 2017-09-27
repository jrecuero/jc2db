import os
import json


class DB_LINK(object):

    def __init__(self, theTable, theField, theValue, theLinkTable, theLinkRow):
        self._linkTable = theLinkTable
        self._linkRow = theLinkRow
        self._table = theTable
        self._field = theField
        self._value = theValue


class DB_TABLE(object):

    __ID = 0

    @classmethod
    def load(cls, theTableName, theFileName=None):
        if theFileName is None:
            filename = os.path.join('_db_', '{0}_db.json'.format(theTableName))
        else:
            filename = theFileName
        filename = '_db_/{0}_db.json'.format(theTableName) if theFileName is None else theFileName
        with open(filename, 'r') as fd:
            data = json.loads(json.load(fd))
            if len(data) == 0:
                return None
            tb = cls(theTableName, data[0])
            for entry in data[1:]:
                row = tb.createRow(**entry)
                tb.Table.append(row)
            return tb

    def __init__(self, theName, theFields):
        self.id = DB_TABLE.__ID + 1
        DB_TABLE.__ID += 1
        self.Name = theName
        self.Fields = theFields
        self.Table = []
        self.RowCb = []
        self.TableCb = []
        self.RowId = 0
        self.RegisterLinks = []
        self.Row = DB_ROW

    def createRow(self, **kwargs):
        _id = kwargs.get('id', None)
        if _id is None:
            self.RowId += 1
            kwargs['id'] = self.RowId
        else:
            if _id > self.RowId:
                self.RowId = _id
        row = self.Row()
        for name, default in self.Fields.items():
            setattr(row, name, default)
        row.update(**kwargs)
        return row

    def registerLink(self, theField, theValue, theLinkTable, theLinkRow):
        self.RegisterLinks.append(DB_LINK(self.Name, theField, theValue, theLinkTable, theLinkRow))

    def save(self, theFileName=None):
        filename = '{0}_db.json'.format(self.Name) if theFileName is None else theFileName
        with open(filename, 'w') as fd:
            data = [self.Fields, ]
            for row in self.Table:
                data.extend([row.getRow(), ])
            json.dump(json.dumps(data), fd)


class DB_ROW(object):

    def __init__(self, **kwargs):
        self.update(**kwargs)

    def getRow(self):
        return self.__dict__

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        st = "\n".join(['{0}: {1}'.format(k, v) for k, v in self.getRow().items()])
        return st + '\n'


class DB(object):

    def __init__(self):
        self._db = {}

    def createTable(self, theTable, theFields):
        tb = DB_TABLE(theTable, theFields)
        self._db.update({theTable: tb})
        return tb

    def getTableByName(self, theTable):
        tb = self._db[theTable]
        return tb

    def getAllTableNames(self):
        for name in self._db.keys():
            yield name

    def addRowToTable(self, theTable, **kwargs):
        tb = self._db[theTable]
        row = tb.createRow(**kwargs)
        tb.Table.append(row)
        for cb in tb.RowCb:
            cb(self, tb, row)
        return row

    def updateRowFromTable(self, theTable, theId, **kwargs):
        tb = self._db[theTable].Table
        for entry in tb:
            if entry.id == theId:
                break
        entry.update(**kwargs)
        return entry

    def deleteRowFromTable(self, theTable, theId):
        tb = self._db[theTable].Table
        for index, entry in enumerate(tb):
            if entry.id == theId:
                break
        del tb[index]

    def getRowFromTable(self, theTable, theId):
        tb = self._db[theTable].Table
        for entry in tb:
            if entry.id == theId:
                break
        return entry

    def getAllRowFromTable(self, theTable):
        tb = self._db[theTable].Table
        for entry in tb:
            yield entry

    def save(self):
        if not os.path.exists('_db_'):
            os.makedirs('_db_')
        for tbName, tb in self._db.items():
            tb.save(os.path.join('_db_', '{0}_db.json'.format(tbName)))

    def load(self):
        if not os.path.exists('_db_'):
            return
        listDir = os.listdir('_db_')
        for fName in listDir:
            tbName = fName.split('/')[-1].split('_db.json')[0]
            tb = DB_TABLE.load(tbName)
            self._db.update({tbName: tb})
