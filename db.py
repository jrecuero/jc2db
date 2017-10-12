import os
import threading
from db_table import DbTable
from db_status import DbStatus


class Db(object):

    __TR_ID = 0

    def __init__(self, name):
        self.name = name
        self._db = {}
        self._shade = {}
        self._tr_pid = {}

    def _get_pid(self):
        return {'pid': os.getid(), 'threadid': threading.get_ident()}

    def _check_pid(self, pid):
        app_pid = self._get_pid()
        return app_pid == pid

    def _check_call_in_tr(self, check_tr=True):
        if check_tr:
            for trid, pid in self._tr_pid.items():
                if self._check_pid(pid):
                    return trid
        return None

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
        shade = self._shade[trid]
        if tablename not in shade:
            self.shade_table(trid, tablename)
        shade_table = shade[tablename]
        if rowid not in shade_table['rows']:
            self.shade_row(trid, tablename, rowid)
        self.shade_update_row(trid, tablename, rowid, **kwargs)

    def _update_row_from_table(self, tablename, rowid, **kwargs):
        tb = self._db[tablename].table
        for row in tb:
            if row.id == rowid:
                break
        row.update(**kwargs)
        self.call_row_cbs('inUpdate', tb, row)
        return row

    def update_row_from_table(self, tablename, rowid, check_tr=True, **kwargs):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_update_row_from_table(tablename, rowid, **kwargs)
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
        self.call_row_cbs('inDelete', tb, row)
        del tb[index]

    def delete_row_from_table(self, tablename, rowid, check_tr=True):
        trid = self._check_call_in_tr(check_tr)
        if trid:
            return self.tr_delete_row_from_table(trid, tablename, rowid)
        else:
            return self._delete_row_from_table(tablename, rowid)

    def tr_get_row_from_table(self, trid, tablename, rowid):
        shade = self._shade[trid]
        row = self.get_row_from_table(tablename, rowid)
        if tablename not in shade:
            return row

        shade_table = shade[tablename]
        if rowid not in shade_table['rows']:
            return row

        shade_row = shade_table['rows'][rowid]
        merged_row = self._merge_rows(row if row else {}, shade_row)
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

    def shade_table(self, trid, tablename):
        fields = self._db[tablename].Fields
        tb = DbTable(tablename, fields)
        self._shade[trid].update({tablename: {'table': tb, 'rows': {}, 'status': DbStatus.UPDATED}})

    def shade_create_table(self, trid, tablename, fields):
        tb = DbTable(tablename, fields)
        self._shade[trid].update({tablename: {'table': tb, 'rows': {}, 'status': DbStatus.CREATED}})

    def shade_delete_table(self, trid, tablename):
        fields = self._db[tablename].Fields
        tb = DbTable(tablename, fields)
        self._shade[trid].update({tablename: {'table': tb, 'rows': {}, 'status': DbStatus.DELETED}})

    def shade_row(self, trid, tablename, rowid):
        shade_table = self._shade[trid][tablename]
        if self.get_row_from_table(tablename, rowid) is None:
            new_row_flag = True
            status = DbStatus.CREATED
        else:
            new_row_flag = False
            status = DbStatus.UPDATED
        row = shade_table['table'].create_shade_row(new_row_flag)
        shade_table['rows'].update({rowid: {'row': row, 'status': status}})

    def shade_delete_row(self, trid, tablename, rowid):
        shade_table = self._shade[trid][tablename]
        row = shade_table['table'].create_shade_row()
        shade_table['rows'].update({rowid: {'row': row, 'status': DbStatus.DELETED}})

    def shade_update_row(self, trid, tablename, rowid, **kwargs):
        shade_table = self._shade[trid][tablename]
        shade_row = shade_table['rows'][rowid]['row']
        shade_row.update(**kwargs)

    def shade_discard(self, trid):
        self._shade[trid] = {}

    def _shade_commit_table_updated(self, tbname, rowid, rowentry):
        if rowentry['status'] == DbStatus.UPDATED:
            self.update_row_from_table(tbname, rowid, **rowentry['row'].getRow())
        elif rowentry['status'] == DbStatus.CREATED:
            raise NotImplementedError
        elif rowentry['status'] == DbStatus.DELETED:
            raise NotImplementedError
        else:
            raise TypeError('Unknow table {0} rowid {1} DbStatus: {2}'.format(tbname, rowid, rowentry['status']))

    def _shade_commit_table_created(self, tbname, rowid, rowentry):
        raise NotImplementedError

    def _shade_commit_table_deleted(self, tbname, rowid, rowentry):
        raise NotImplementedError

    def shade_commit(self, trid):
        for tbname, tbentry in self._shade[trid].items():
            for rowid, rowentry in tbentry['rows'].items():
                if tbentry['status'] == DbStatus.UPDATED:
                    self._shade_commit_table_updated(tbname, rowid, rowentry)
                elif tbentry['status'] == DbStatus.CREATED:
                    self._shade_commit_table_created(tbname, rowid, rowentry)
                elif tbentry['status'] == DbStatus.DELETED:
                    self._shade_commit_table_deleted(tbname, rowid, rowentry)
                else:
                    raise TypeError('Unknow table {0} DbStatus: {1}'.format(tbname, tbentry['status']))
        self.shade_discard(trid)

    def tr_create(self):
        self.Db__TR_ID += 1
        trid = self.Db__TR_ID
        self._shade.update({trid: {}})
        self._tr_pid.update({trid: self._get_pid()})
        return trid

    def _merge_rows(self, main_row, updated_row):
        merge_row = main_row.copy()
        merge_row.update(updated_row)
        return merge_row

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
