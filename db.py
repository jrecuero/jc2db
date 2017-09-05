class DB_LINK(object):

    def __init__(self, theTable, theField, theValue, theLinkTable, theLinkRow):
        self._linkTable = theLinkTable
        self._linkRow = theLinkRow
        self._table = theTable
        self._field = theField
        self._value = theValue


class DB_TABLE(object):

    __ID = 0

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
        self.RowId += 1
        kwargs['id'] = self.RowId
        row = self.Row()
        for name, default in self.Fields.items():
            setattr(row, name, default)
        row.update(**kwargs)
        return row

    def registerLink(self, theField, theValue, theLinkTable, theLinkRow):
        self.RegisterLinks.append(DB_LINK(self.Name, theField, theValue, theLinkTable, theLinkRow))


class DB_ROW(object):

    def __init__(self, **kwargs):
        self.update(**kwargs)

    def update(self, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __repr__(self):
        return "\n".join(['{0}: {1}'.format(k, v) for k, v in self.__dict__.items()])


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
