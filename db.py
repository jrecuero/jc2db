import os
from db_table import DbTable


class Db(object):

    __TR_ID = 0

    def __init__(self, name):
        self.name = name
        self._db = {}
        self._shade = {}

    def create_table(self, tablename, fields):
        tb = DbTable(tablename, fields)
        self._db.update({tablename: tb})
        return tb

    def get_table_by_name(self, tablename):
        tb = self._db[tablename]
        return tb

    def get_all_table_names(self):
        for name in self._db.keys():
            yield name

    def call_row_cbs(self, condition, table, row):
        for notif in table.RowCb.values():
            if getattr(notif, condition)():
                notif.Cb(self, table, row)

    def add_row_to_table(self, tablename, **kwargs):
        tb = self._db[tablename]
        row = tb.createRow(**kwargs)
        tb.table.append(row)
        self.call_row_cbs('inCreate', tb, row)
        return row

    def update_row_from_table(self, tablename, id, **kwargs):
        tb = self._db[tablename].table
        for row in tb:
            if row.id == id:
                break
        row.update(**kwargs)
        self.call_row_cbs('inUpdate', tb, row)
        return row

    def delete_row_from_table(self, tablename, id):
        tb = self._db[tablename].table
        for index, row in enumerate(tb):
            if row.id == id:
                break
        self.call_row_cbs('inDelete', tb, row)
        del tb[index]

    def get_row_from_table(self, tablename, id):
        tb = self._db[tablename].table
        for row in tb:
            if row.id == id:
                return row
        return None

    def get_all_row_from_table(self, tablename):
        tb = self._db[tablename].table
        for row in tb:
            yield row

    def shade_table(self, trid, tablename):
        fields = self._db[tablename].Fields
        tb = DbTable(tablename, fields)
        self._shade[trid].update({tablename: {'table': tb, 'rows': {}}})

    def shade_row(self, trid, tablename, rowid):
        shade_table = self._shade[trid][tablename]
        newRowFlag = True if self.get_row_from_table(tablename, rowid) is None else False
        row = shade_table['table'].createShadeRow(newRowFlag)
        shade_table['rows'].update({rowid: row})

    def shade_update_row(self, trid, tablename, rowid, **kwargs):
        shade_table = self._shade[trid][tablename]
        shadeRow = shade_table['rows'][rowid]
        shadeRow.update(**kwargs)

    def shade_discard(self, trid):
        self._shade[trid] = {}

    def shade_commit(self, trid):
        for tbname, tbentry in self._shade[trid].items():
            for rowid, row in tbentry['rows'].items():
                self.update_row_from_table(tbname, rowid, **row.getRow())
        self.shade_discard(trid)

    def tr_create(self):
        self.Db__TR_ID += 1
        id = self.Db__TR_ID
        self._shade.update({id: {}})
        return id

    def tr_update_row_from_table(self, trid, tablename, rowid, **kwargs):
        shade = self._shade[trid]
        if tablename not in shade:
            self.shade_table(trid, tablename)
        shade_table = shade[tablename]
        if rowid not in shade_table['rows']:
            self.shadeRow(trid, tablename, rowid)
        self.shade_update_row(trid, tablename, rowid, **kwargs)

    def tr_discard(self, trid):
        self.shade_discard(trid)

    def tr_commit(self, trid):
        self.shade_commit(trid)

    def tr_close(self, trid, commit_flag=True):
        if commit_flag:
            self.tr_commit(trid)
        else:
            self.tr_discard(trid)
        del self._shade[trid]

    def save(self):
        if not os.path.exists('_db_'):
            os.makedirs('_db_')
        path = os.path.join('_db_', self.name)
        if not os.path.exists(path):
            os.makedirs(path)
        for tbName, tb in self._db.items():
            tb.save(os.path.join(path, '{0}_db.json'.format(tbName)))

    def load(self):
        path = os.path.join('_db_', self.name)
        if not os.path.exists(path):
            return
        list_dir = os.listdir(path)
        for filename in list_dir:
            tbName = filename.split('/')[-1].split('_db.json')[0]
            tb = DbTable.load(tbName, os.path.abspath(os.path.join(path, filename)))
            self._db.update({tbName: tb})
