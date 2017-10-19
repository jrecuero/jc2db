from enum import Flag, auto


class Notif(Flag):

    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()
    ALL = CREATE | UPDATE | DELETE


class DbNotif(object):

    def __init__(self, name, notif, cb):
        self.name = name
        self.notif = notif
        self.Cb = cb

    def in_create(self):
        return bool(self.notif & Notif.CREATE)

    def in_update(self):
        return bool(self.notif & Notif.UPDATE)

    def in_delete(self):
        return bool(self.notif & Notif.DELETE)


class DbLink(object):

    def __init__(self, table_name, field, value, link_table, link_row):
        self.link_table = link_table
        self.link_row = link_row
        self.table = table_name
        self.field = field
        self.value = value
