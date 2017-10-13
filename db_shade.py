from db_status import DbStatus
from db_table import DbTable


class DbShade(object):

    def __init__(self, main_db):
        self._dbase = main_db
        self._db = {}

    def new_tr(self, trid):
        self._db.update({trid: {}})

    def del_tr(self, trid):
        del self._db[trid]

    def get_table_from_tr(self, trid, tablename):
        if self.is_table_in_tr(trid, tablename):
            return self._db[trid][tablename]
        else:
            return None

    def get_row_from_tr(self, trid, tablename, rowid):
        if self.is_row_in_tr(trid, tablename, rowid):
            return self._db[trid][tablename]['rows'][rowid]['row']
        else:
            return None

    def is_table_in_tr(self, trid, tablename):
        return trid in self._db and tablename in self._db[trid]

    def is_row_in_tr(self, trid, tablename, rowid):
        return self.is_table_in_tr(trid, tablename) and rowid in self._db[trid][tablename]['rows']

    def add_table_updated(self, trid, tablename):
        fields = self._dbase.get_table_by_name(tablename, False)
        tb = DbTable(tablename, fields)
        tb_entry = {tablename: {'table': tb, 'rows': {}, 'status': DbStatus.UPDATED}}
        self._db[trid].update(tb_entry)
        return tb_entry[tablename]

    def add_table_created(self, trid, tablename, fields):
        tb = DbTable(tablename, fields)
        tb_entry = {tablename: {'table': tb, 'rows': {}, 'status': DbStatus.CREATED}}
        self._db[trid].update(tb_entry)
        return tb_entry[tablename]

    def add_table_deleted(self, trid, tablename):
        fields = self._dbase.get_table_by_name(tablename, False)
        tb = DbTable(tablename, fields)
        tb_entry = {tablename: {'table': tb, 'rows': {}, 'status': DbStatus.DELETED}}
        self._db[trid].update(tb_entry)
        return tb_entry[tablename]

    def add_row_updated(self, trid, tablename, rowid):
        shade_table = self._db[trid][tablename]
        row = shade_table['table'].create_shade_row(False)
        row_entry = {rowid: {'row': row, 'status': DbStatus.UPDATED}}
        shade_table['rows'].update(row_entry)
        return row_entry

    def add_row_created(self, trid, tablename, rowid):
        shade_table = self._db[trid][tablename]
        row = shade_table['table'].create_shade_row(True)
        row_entry = {rowid: {'row': row, 'status': DbStatus.CREATED}}
        shade_table['rows'].update(row_entry)
        return row_entry

    def add_row_deleted(self, trid, tablename, rowid):
        shade_table = self._db[trid][tablename]
        row = shade_table['table'].create_shade_row()
        row_entry = {rowid: {'row': row, 'status': DbStatus.DELETED}}
        shade_table['rows'].update(row_entry)
        return row_entry

    def update_row(self, trid, tablename, rowid, **kwargs):
        shade_table = self._db[trid][tablename]
        shade_row = shade_table['rows'][rowid]['row']
        shade_row.update(**kwargs)

    def discard(self, trid):
        self._db[trid] = {}
        return True

    def _commit_table_updated(self, tbname, rowid, rowentry):
        if rowentry['status'] == DbStatus.UPDATED:
            self._dbase.update_row_from_table(tbname, rowid, **rowentry['row'].getRow())
        elif rowentry['status'] == DbStatus.CREATED:
            raise NotImplementedError
        elif rowentry['status'] == DbStatus.DELETED:
            raise NotImplementedError
        else:
            raise TypeError('Unknow table {0} rowid {1} DbStatus: {2}'.format(tbname, rowid, rowentry['status']))

    def _commit_table_created(self, tbname, rowid, rowentry):
        raise NotImplementedError

    def _commit_table_deleted(self, tbname, rowid, rowentry):
        raise NotImplementedError

    def commit(self, trid):
        for tbname, tbentry in self._db[trid].items():
            for rowid, rowentry in tbentry['rows'].items():
                if tbentry['status'] == DbStatus.UPDATED:
                    self._commit_table_updated(tbname, rowid, rowentry)
                elif tbentry['status'] == DbStatus.CREATED:
                    self._commit_table_created(tbname, rowid, rowentry)
                elif tbentry['status'] == DbStatus.DELETED:
                    self._commit_table_deleted(tbname, rowid, rowentry)
                else:
                    raise TypeError('Unknow table {0} DbStatus: {1}'.format(tbname, tbentry['status']))
        self.discard(trid)
        return True
