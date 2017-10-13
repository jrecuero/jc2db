import os
import json
from db_notif import DbNotif, DbLink
from db_row import DbRow


class DbTable(object):

    __ID = 0

    @classmethod
    def load(cls, tablename, filename=None, default_path='_db_'):
        if filename is None:
            filename = os.path.join(default_path, '{0}_db.json'.format(tablename))
        else:
            filename = filename
        filename = '{0}/{1}_db.json'.format(default_path, tablename) if filename is None else filename
        with open(filename, 'r') as fd:
            data = json.loads(json.load(fd))
            if len(data) == 0:
                return None
            tb = cls(tablename, data[0])
            for row in data[1:]:
                row = tb.create_row(**row)
                tb.table.append(row)
            return tb

    def __init__(self, name, fields):
        self.id = DbTable.__ID + 1
        DbTable.__ID += 1
        self.name = name
        self.fields = fields
        self.table = []
        self._row_cb = {}
        self._table_cb = {}
        self._row_id = 0
        self._registered_links = []
        self.row = DbRow

    def create_row(self, **kwargs):
        id = kwargs.get('id', None)
        if id is None:
            self._row_id += 1
            kwargs['id'] = self._row_id
        else:
            if id > self._row_id:
                self._row_id = id
        row = self.row()
        for name, default in self.fields.items():
            setattr(row, name, default)
        row.update(**kwargs)
        return row

    def create_shade_row(self, new_flag=False):
        kwargs = {}
        if new_flag:
            self._row_id += 1
            kwargs['id'] = self._row_id
        row = self.row()
        row.update(**kwargs)
        return row

    def register_row_cb(self, name, notif, cb):
        notif = DbNotif(name, notif, cb)
        self._row_cb.update({name: notif})

    def unregister_row_cb(self, name):
        del self._row_cb[name]

    def register_link(self, field, value, link_table, link_row):
        self._registered_links.append(DbLink(self.name, field, value, link_table, link_row))

    def save(self, filename=None, default_path='_db_'):
        filename = '{0}_db.json'.format(self.name) if filename is None else filename
        with open(filename, 'w') as fd:
            data = [self.fields, ]
            for row in self.table:
                data.extend([row.get_row(), ])
            json.dump(json.dumps(data), fd)

    def get_as_str(self, pattern):
        st = self.table[0].get_as_str(pattern, True)
        for row in self.table:
            st += row.get_as_str(pattern)
        return st
