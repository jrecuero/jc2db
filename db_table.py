import os
import json
from db_notif import DB_NOTIF, DB_LINK
from db_row import DB_ROW


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
            for row in data[1:]:
                row = tb.createRow(**row)
                tb.Table.append(row)
            return tb

    def __init__(self, theName, theFields):
        self.id = DB_TABLE.__ID + 1
        DB_TABLE.__ID += 1
        self.Name = theName
        self.Fields = theFields
        self.Table = []
        self.RowCb = {}
        self.TableCb = {}
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

    def registerRowCb(self, theName, theNotif, theCb):
        notif = DB_NOTIF(theName, theNotif, theCb)
        self.RowCb.update({theName: notif})

    def unregisterRowCb(self, theName):
        del self.RowCb[theName]

    def registerLink(self, theField, theValue, theLinkTable, theLinkRow):
        self.RegisterLinks.append(DB_LINK(self.Name, theField, theValue, theLinkTable, theLinkRow))

    def save(self, theFileName=None):
        filename = '{0}_db.json'.format(self.Name) if theFileName is None else theFileName
        with open(filename, 'w') as fd:
            data = [self.Fields, ]
            for row in self.Table:
                data.extend([row.getRow(), ])
            json.dump(json.dumps(data), fd)

    def getAsStr(self, thePattern):
        st = self.Table[0].getAsStr(thePattern, True)
        for row in self.Table:
            st += row.getAsStr(thePattern)
        return st
