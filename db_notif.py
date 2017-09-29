from enum import Flag, auto


class E_Notif(Flag):

    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()
    ALL = CREATE | UPDATE | DELETE


class DB_NOTIF(object):

    def __init__(self, theName, theNotif, theCb):
        self.Name = theName
        self.Notif = theNotif
        self.Cb = theCb

    def inCreate(self):
        return bool(self.Notif & E_Notif.CREATE)

    def inUpdate(self):
        return bool(self.Notif & E_Notif.UPDATE)

    def inDelete(self):
        return bool(self.Notif & E_Notif.DELETE)


class DB_LINK(object):

    def __init__(self, theTableName, theField, theValue, theLinkTable, theLinkRow):
        self._linkTable = theLinkTable
        self._linkRow = theLinkRow
        self._table = theTableName
        self._field = theField
        self._value = theValue
