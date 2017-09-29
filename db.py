import os
from db_table import DB_TABLE


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

    def callRowCbs(self, theCondition, theTb, theRow):
        for notif in theTb.RowCb.values():
            if getattr(notif, theCondition)():
                notif.Cb(self, theTb, theRow)

    def addRowToTable(self, theTableName, **kwargs):
        tb = self._db[theTableName]
        row = tb.createRow(**kwargs)
        tb.Table.append(row)
        self.callRowCbs('inCreate', tb, row)
        return row

    def updateRowFromTable(self, theTableName, theId, **kwargs):
        tb = self._db[theTableName].Table
        for row in tb:
            if row.id == theId:
                break
        row.update(**kwargs)
        self.callRowCbs('inUpdate', tb, row)
        return row

    def deleteRowFromTable(self, theTableName, theId):
        tb = self._db[theTableName].Table
        for index, row in enumerate(tb):
            if row.id == theId:
                break
        self.callRowCbs('inDelete', tb, row)
        del tb[index]

    def getRowFromTable(self, theTableName, theId):
        tb = self._db[theTableName].Table
        for row in tb:
            if row.id == theId:
                return row
        return None

    def getAllRowFromTable(self, theTableName):
        tb = self._db[theTableName].Table
        for row in tb:
            yield row

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
