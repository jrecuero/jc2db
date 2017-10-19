import pytest
from jc2db.db import Db
from jc2db.db_shade import DbShade


@pytest.fixture(params=['test'])
def dbase(request):
    return Db(request.param)


@pytest.fixture(params=[['Person', {'name': '-', 'age': 0}]])
def table(request, dbase):
    table_name = request.param[0]
    fields = request.param[1]
    return dbase.create_table(table_name, fields, False)


@pytest.fixture(params=[(('Person', 'Company'),
                         ({'name': '-', 'age': 0},
                          {'cname': '*', 'country': '$', 'capital': 1000}))])
def tables(request, dbase):
    new_tables = []
    table_names = request.param[0]
    fieldss = request.param[1]
    for table_name, fields in zip(table_names, fieldss):
        new_tables.append(dbase.create_table(table_name, fields, False))
    return new_tables


@pytest.fixture(params=[[{'name': 'foo', 'age': 20},
                         {'name': 'bar', 'age': 30},
                         {'name': 'zap', 'age': 40},
                         {'name': 'plo', 'age': 50}]])
def tb1rows(request, dbase, table):
    new_rows = []
    for row in request.param:
        new_rows.append(dbase.add_row_to_table(table.name, False, **row))
    return new_rows


@pytest.fixture(params=[(('Person', 'Company'),
                         (({'name': 'foo', 'age': 20},
                           {'name': 'bar', 'age': 30},
                           {'name': 'zap', 'age': 40},
                           {'name': 'plo', 'age': 50}),
                          ({'cname': 'ABC', 'country': 'USA', 'capital': 500},
                           {'cname': 'ZXY', 'country': 'EU', 'capital': 250})))])
def tb2rows(request, dbase, tables):
    table_names = request.param[0]
    rowss = request.param[1]
    for table_name, rows in zip(table_names, rowss):
        for row in rows:
            dbase.add_row_to_table(table_name, False, **row)


class TestDb(object):

    @pytest.mark.parametrize('dbase', ['test'], indirect=True)
    @pytest.mark.parametrize('db_name', ['test'])
    def test_create_db(self, dbase, db_name):
        assert dbase.name == db_name
        assert dbase._db == {}
        assert dbase._tr_pid == {}
        assert isinstance(dbase._shade, DbShade)

    @pytest.mark.parametrize('table_name', ['Person'])
    @pytest.mark.parametrize('fields', [{'name': '-', 'age': 0}])
    def test_create_table(self, dbase, table_name, fields):
        dbase.create_table(table_name, fields, False)
        assert table_name in dbase._db
        assert len(dbase._db) == 1

    @pytest.mark.parametrize('table_names, fieldss',
                             [(('Person', 'Company'),
                               ({'name': '-', 'age': 0},
                                {'cname': '*', 'country': '$', 'capital': 1000}))],)
    def test_create_multiple_tables(self, dbase, table_names, fieldss):
        for table_name, fields in zip(table_names, fieldss):
            dbase.create_table(table_name, fields, False)
            assert table_name in dbase._db
        assert len(dbase._db) == 2
        index = 0
        for name in dbase.get_all_table_names(False):
            assert table_names[index] == name
            index += 1

    def test_get_all_table_names(self, dbase, tables):
        index = 0
        for name in dbase.get_all_table_names(False):
            assert tables[index].name == name
            index += 1

    @pytest.mark.parametrize('row', [{'name': 'foo', 'age': 50}])
    @pytest.mark.parametrize('rowid', [1])
    def test_create_row(self, dbase, table, row, rowid):
        new_row = dbase.add_row_to_table(table.name, False, **row)
        assert new_row == dbase.get_row_from_table(table.name, rowid, False)

    @pytest.mark.parametrize('row_list', [[{'name': 'foo', 'age': 20},
                                           {'name': 'bar', 'age': 30},
                                           {'name': 'zap', 'age': 40},
                                           {'name': 'plo', 'age': 50}]])
    def test_create_multiple_row(self, dbase, table, row_list):
        new_rows = []
        for row in row_list:
            new_rows.append(dbase.add_row_to_table(table.name, False, **row))

        for rowid, row in enumerate(new_rows):
            assert row == dbase.get_row_from_table(table.name, rowid + 1, False)

    def test_get_all_rows(self, dbase, table, tb1rows):
        index = 0
        for row in dbase.get_all_row_from_table(table.name, False):
            assert row == tb1rows[index]
            index += 1

    @pytest.mark.parametrize("rowid, updates", [(1, {'age': 21}),
                                                (2, {'name': 'BAR'}),
                                                (4, {'name': 'PLO', 'age': 51})])
    def test_update_row(self, dbase, table, tb1rows, rowid, updates):
        dbase.update_row_from_table(table.name, rowid, False, **updates)
        row = dbase.get_row_from_table(table.name, rowid, False)
        for key, value in updates.items():
            assert getattr(row, key) == value

    @pytest.mark.parametrize("rowids, nbr_rows", [((1, ), 3), ((2, 3), 2)])
    def test_delete_row(self, dbase, table, tb1rows, rowids, nbr_rows):
        for rowid in rowids:
            assert dbase.get_row_from_table(table.name, rowid)
            dbase.delete_row_from_table(table.name, rowid, False)
            assert dbase.get_row_from_table(table.name, rowid) is None
        assert nbr_rows == len(dbase.get_table_by_name(table.name).table)

    def test_load_and_save_db(self, dbase, tables):
        assert dbase.save('_test_')
        table_names = {}
        for name in dbase.get_all_table_names(False):
            table_names.update({name: True})
        db_name = dbase.name
        new_dbase = Db(db_name)
        assert new_dbase.name == db_name
        assert new_dbase._db == {}
        assert new_dbase._tr_pid == {}
        assert isinstance(new_dbase._shade, DbShade)
        assert new_dbase.load('_test_')
        assert len(new_dbase._db) == 2
        new_table_names = {}
        for name in new_dbase.get_all_table_names(False):
            new_table_names.update({name: True})
        assert table_names == new_table_names

    def test_fail_load(self, dbase, tables):
        assert dbase.save('_test_')
        fail_dbase = Db('fail')
        assert not fail_dbase.load()

    @pytest.mark.parametrize("trid", [1])
    def test_create_rw(self, dbase, tables, tb2rows, trid):
        assert trid == dbase.tr_create()
        assert dbase.is_in_tr()

    @pytest.mark.parametrize("rowid, updates", [(1, {'age': 21}),
                                                (2, {'name': 'BAR'}),
                                                (4, {'name': 'PLO', 'age': 51})])
    def test_tr_update_row(self, dbase, table, tb1rows, rowid, updates):
        dbase.tr_create()
        dbase.update_row_from_table(table.name, rowid, True, **updates)
        row_in_tr = dbase.get_row_from_table(table.name, rowid, True)
        for key, value in updates.items():
            assert getattr(row_in_tr, key) == value

    @pytest.mark.parametrize("rowid, updates", [(1, {'age': 21}),
                                                (2, {'name': 'BAR'}),
                                                (4, {'name': 'PLO', 'age': 51})])
    def test_tr_update_row_out_tr(self, dbase, table, tb1rows, rowid, updates):
        original_row = dbase.get_row_from_table(table.name, rowid, False)
        dbase.tr_create()
        dbase.update_row_from_table(table.name, rowid, True, **updates)
        row_out_tr = dbase.get_row_from_table(table.name, rowid, False)
        assert original_row == row_out_tr
        for key, value in updates.items():
            assert getattr(row_out_tr, key) != value

    @pytest.mark.parametrize("trid", [1])
    def test_tr_commit(self, dbase, table, trid):
        assert trid == dbase.tr_create()
        assert dbase.tr_commit(trid)

    @pytest.mark.parametrize("trid", [1])
    def test_tr_discard(self, dbase, table, trid):
        assert trid == dbase.tr_create()
        assert dbase.tr_discard(trid)

    @pytest.mark.parametrize("trid", [1])
    def test_tr_close_commit(self, dbase, table, trid):
        assert trid == dbase.tr_create()
        assert dbase.tr_close(trid, True)

    @pytest.mark.parametrize("trid", [1])
    def test_tr_close_discard(self, dbase, table, trid):
        assert trid == dbase.tr_create()
        assert dbase.tr_close(trid, False)
