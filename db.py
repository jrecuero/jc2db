import os
import threading
from db_table import DbTable
from db_shade import DbShade


class Db(object):

    __TR_ID = 0

    def __init__(self, name):
        self.name = name
        self._db = {}
        self._shade = DbShade(self)
        self._tr_pid = {}

    def _get_pid(self):
        return {'pid': os.getpid(), 'threadid': threading.get_ident()}

    def _check_pid(self, pid):
        app_pid = self._get_pid()
        return app_pid == pid

    def _check_call_in_tr(self, check_tr=True):
        if check_tr:
            for trid, pid in self._tr_pid.items():
                if self._check_pid(pid):
                    return trid
        return None

    def is_in_tr(self):
        return self._check_call_in_tr()

    def tr_create_table(self, trid, tablename, fields):
        # TODO: TBD
        return self._create_table(tablename, fields)

    def _create_table(self, tablename, fields):
        tb = DbTable(tablename, fields)
        self._db.update({tablename: tb})
        return tb

    def create_table(self, tablename, fields, check_tr=True):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_create_table(trid, tablename, fields)
        else:
            return self._create_table(tablename, fields)

    def tr_get_table_by_name(self, trid, tablename):
        # TODO: TBD
        return self._get_table_by_name(tablename)

    def _get_table_by_name(self, tablename):
        tb = self._db[tablename]
        return tb

    def get_table_by_name(self, tablename, check_tr=True):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_get_table_by_name(tablename)
        else:
            return self._get_table_by_name(tablename)

    def tr_get_all_table_names(self, trid):
        # TODO: TBD
        return self._get_all_table_names()

    def _get_all_table_names(self):
        for name in self._db.keys():
            yield name

    def get_all_table_names(self, check_tr=True):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self._get_all_table_names(trid)
        else:
            return self._get_all_table_names()

    def call_row_cbs(self, condition, table, row):
        for notif in table._row_cb.values():
            if getattr(notif, condition)():
                notif.Cb(self, table, row)

    def tr_add_row_to_table(self, trid, tablename, **kwargs):
        # TODO: TBD
        return self._add_row_to_table(tablename, **kwargs)

    def _add_row_to_table(self, tablename, **kwargs):
        tb = self._db[tablename]
        row = tb.create_row(**kwargs)
        tb.table.append(row)
        self.call_row_cbs('inCreate', tb, row)
        return row

    def add_row_to_table(self, tablename, check_tr=True, **kwargs):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_add_row_to_table(trid, tablename, **kwargs)
        else:
            return self._add_row_to_table(tablename, **kwargs)

    def tr_update_row_from_table(self, trid, tablename, rowid, **kwargs):
        shade_table = self._shade.get_table_from_tr(trid, tablename)
        if shade_table is None:
            shade_table = self._shade.add_table_updated(trid, tablename)
        if rowid not in shade_table['rows']:
            self._shade.add_row_updated(trid, tablename, rowid)
        self._shade.update_row(trid, tablename, rowid, **kwargs)

    def _update_row_from_table(self, tablename, rowid, **kwargs):
        tb = self._db[tablename].table
        for row in tb:
            if row.id == rowid:
                break
        row.update(**kwargs)
        self.call_row_cbs('inUpdate', self._db[tablename], row)
        return row

    def update_row_from_table(self, tablename, rowid, check_tr=True, **kwargs):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_update_row_from_table(trid, tablename, rowid, **kwargs)
        else:
            return self._update_row_from_table(tablename, rowid, **kwargs)

    def tr_delete_row_from_table(self, tablename, rowid):
        # TODO: TBD
        return self._delete_row_from_table(tablename, rowid)

    def _delete_row_from_table(self, tablename, rowid):
        tb = self._db[tablename].table
        for index, row in enumerate(tb):
            if row.id == rowid:
                break
        self.call_row_cbs('inDelete', self._db[tablename], row)
        del tb[index]

    def delete_row_from_table(self, tablename, rowid, check_tr=True):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_delete_row_from_table(trid, tablename, rowid)
        else:
            return self._delete_row_from_table(tablename, rowid)

    def tr_get_row_from_table(self, trid, tablename, rowid):
        shade_row = self._shade.get_row_from_tr(trid, tablename, rowid)
        row = self.get_row_from_table(tablename, rowid, False)
        if shade_row is None:
            return row
        merged_row = self._merge_rows(row, shade_row)
        return merged_row

    def _get_row_from_table(self, tablename, rowid):
        tb = self._db[tablename].table
        for row in tb:
            if row.id == rowid:
                return row
        return None

    def get_row_from_table(self, tablename, rowid, check_tr=True):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_get_row_from_table(trid, tablename, rowid)
        else:
            return self._get_row_from_table(tablename, rowid)

    def tr_get_all_row_from_table(self, trid, tablename):
        # TODO: TBD
        return self.get_all_row_from_table(tablename)

    def _get_all_row_from_table(self, tablename):
        tb = self._db[tablename].table
        for row in tb:
            yield row

    def get_all_row_from_table(self, tablename, check_tr=True):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_get_all_row_from_table(trid, tablename)
        else:
            return self._get_all_row_from_table(tablename)

    def tr_create(self):
        self.__TR_ID += 1
        trid = self.__TR_ID
        self._shade.new_tr(trid)
        self._tr_pid.update({trid: self._get_pid()})
        return trid

    def _merge_rows(self, main_row, updated_row):
        if main_row:
            merge_row = main_row.clone()
            merge_row.update(**updated_row.get_row())
        else:
            merge_row = updated_row
        return merge_row

    def tr_discard(self, trid):
        return self._shade.discard(trid)

    def tr_commit(self, trid):
        return self._shade.commit(trid)

    def tr_close(self, trid, commit_flag=True):
        if commit_flag:
            ret = self.tr_commit(trid)
        else:
            ret = self.tr_discard(trid)
        self._shade.del_tr(trid)
        return ret

    def save(self, default_path='_db_'):
        if not os.path.exists(default_path):
            os.makedirs(default_path)
        path = os.path.join(default_path, self.name)
        if not os.path.exists(path):
            os.makedirs(path)
        for tbName, tb in self._db.items():
            tb.save(os.path.join(path, '{0}_db.json'.format(tbName)), default_path)
        return True

    def load(self, default_path='_db_'):
        path = os.path.join(default_path, self.name)
        if not os.path.exists(path):
            return False
        list_dir = os.listdir(path)
        for filename in list_dir:
            tbName = filename.split('/')[-1].split('_db.json')[0]
            tb = DbTable.load(tbName, os.path.abspath(os.path.join(path, filename)), default_path)
            self._db.update({tbName: tb})
        return True
