import os
import json


class DB_LINK(object):

    def __init__(self, theTableName, theField, theValue, theLinkTable, theLinkRow):
        self._linkTable = theLinkTable
        self._linkRow = theLinkRow
        self._table = theTableName
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

    def createShadeRow(self, theNewFlag=False):
        kwargs = {}
        if theNewFlag:
            self.RowId += 1
            kwargs['id'] = self.RowId
        row = self.Row()
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

    def clone(self):
        return self.__class__(**self.__dict__)

    def __repr__(self):
        st = "\n".join(['{0}: {1}'.format(k, v) for k, v in self.getRow().items()])
        return st + '\n'


class DB(object):

    __TR_ID = 0

    def __init__(self, theName):
        self.Name = theName
        self._db = {}
        self._shade = {}

    def createTable(self, theTableName, theFields):
        tb = DB_TABLE(theTableName, theFields)
        self._db.update({theTableName: tb})
        return tb

    def getTableByName(self, theTableName):
        tb = self._db[theTableName]
        return tb

    def getAllTableNames(self):
        for name in self._db.keys():
            yield name

    def addRowToTable(self, theTableName, **kwargs):
        tb = self._db[theTableName]
        row = tb.createRow(**kwargs)
        tb.Table.append(row)
        for cb in tb.RowCb:
            cb(self, tb, row)
        return row

    def updateRowFromTable(self, theTableName, theId, **kwargs):
        tb = self._db[theTableName].Table
        for entry in tb:
            if entry.id == theId:
                break
        entry.update(**kwargs)
        return entry

    def deleteRowFromTable(self, theTableName, theId):
        tb = self._db[theTableName].Table
        for index, entry in enumerate(tb):
            if entry.id == theId:
                break
        del tb[index]

    def getRowFromTable(self, theTableName, theId):
        tb = self._db[theTableName].Table
        for entry in tb:
            if entry.id == theId:
                return entry
        return None

    def getAllRowFromTable(self, theTableName):
        tb = self._db[theTableName].Table
        for entry in tb:
            yield entry

    def shadeTable(self, theTrId, theTableName):
        fields = self._db[theTableName].Fields
        tb = DB_TABLE(theTableName, fields)
        self._shade[theTrId].update({theTableName: {'table': tb, 'rows': {}}})

    def shadeRow(self, theTrId, theTableName, theRowId):
        shadeTable = self._shade[theTrId][theTableName]
        newRowFlag = True if self.getRowFromTable(theTableName, theRowId) is None else False
        row = shadeTable['table'].createShadeRow(newRowFlag)
        shadeTable['rows'].update({theRowId: row})

    def shadeUpdateRow(self, theTrId, theTableName, theRowId, **kwargs):
        shadeTable = self._shade[theTrId][theTableName]
        shadeRow = shadeTable['rows'][theRowId]
        shadeRow.update(**kwargs)

    def shadeDiscard(self, theTrId):
        self._shade[theTrId] = {}

    def shadeCommit(self, theTrId):
        for tbname, tbentry in self._shade[theTrId].items():
            for rowid, row in tbentry['rows'].items():
                self.updateRowFromTable(tbname, rowid, **row.getRow())
        self.shadeDiscard(theTrId)

    def trCreate(self):
        self._DB__TR_ID += 1
        _id = self._DB__TR_ID
        self._shade.update({_id: {}})
        return _id

    def trUpdateRowFromTable(self, theTrId, theTableName, theRowId, **kwargs):
        shade = self._shade[theTrId]
        if theTableName not in shade:
            self.shadeTable(theTrId, theTableName)
        shadeTb = shade[theTableName]
        if theRowId not in shadeTb['rows']:
            self.shadeRow(theTrId, theTableName, theRowId)
        self.shadeUpdateRow(theTrId, theTableName, theRowId, **kwargs)

    def trDiscard(self, theTrId):
        self.shadeDiscard(theTrId)

    def trCommit(self, theTrId):
        self.shadeCommit(theTrId)

    def trClose(self, theTrId, theCommit=True):
        if theCommit:
            self.trCommit(theTrId)
        else:
            self.trDiscard(theTrId)
        del self._shade[theTrId]

    def save(self):
        if not os.path.exists('_db_'):
            os.makedirs('_db_')
        path = os.path.join('_db_', self.Name)
        if not os.path.exists(path):
            os.makedirs(path)
        for tbName, tb in self._db.items():
            tb.save(os.path.join(path, '{0}_db.json'.format(tbName)))

    def load(self):
        path = os.path.join('_db_', self.Name)
        if not os.path.exists(path):
            return
        listDir = os.listdir(path)
        for fName in listDir:
            tbName = fName.split('/')[-1].split('_db.json')[0]
            tb = DB_TABLE.load(tbName, os.path.abspath(os.path.join(path, fName)))
            self._db.update({tbName: tb})
