import sys
sys.path.append('../jc2li')

from base import Cli
from decorators import argo, syntax, setsyntax
from argtypes import Str, Int, Dicta
import loggerator
from db import Db


class Dbase(Cli):

    def __init__(self):
        super(Dbase, self).__init__()
        self._logger = loggerator.getLoggerator('Dbase')
        self._db = None

    def _argos_to_dicta(self, argos):
        dicta = {}
        for arg in argos:
            pname, pval = arg.split('=')
            dicta.update({pname: pval})
        return dicta

    @Cli.command()
    @setsyntax
    @syntax('CREATE-DBASE name')
    @argo('name', Str, None)
    def do_create_dbase(self, name):
        """Create a new database.
        """
        print ('create-dbase {0}'.format(name))
        self._db = Db(name)

    @Cli.command()
    @setsyntax
    @syntax('CREATE-TABLE tbname [dicta]@')
    @argo('tbname', Str, None)
    @argo('dicta', Dicta, {})
    def do_create_table(self, tbname, dicta):
        """Create a new database table in free format.
        """
        print('create-table {0} {1}'.format(tbname, dicta))
        self._db.create_table(tbname, dicta)

    @Cli.command()
    @setsyntax
    @syntax('CREATE-ROW tbname dicta')
    @argo('tbname', Str, None)
    @argo('dicta', Dicta, {})
    def do_create_row(self, tbname, dicta):
        """Create a new table row in free format.
        """
        print('create-row {0} {1}'.format(tbname, dicta))
        self._db.add_row_to_table(tbname, **dicta)

    @Cli.command('CREATE-TR')
    def do_create_tr(self, *args):
        """Create a new database transaction.
        """
        _id = self._db.tr_create()
        print('create-tr {0}'.format(_id))

    @Cli.command()
    @setsyntax
    @syntax('UPDATE-ROW tbname rowid dicta')
    @argo('tbname', Str, None)
    @argo('rowid', Int, None)
    @argo('dicta', Dicta, {})
    def do_update_row(self, tbname, rowid, dicta):
        """Update a given row from a table.
        """
        print('update-row {0} {1} {2}'.format(tbname, rowid, dicta))
        self._db.update_row_from_table(tbname, rowid, **dicta)

    @Cli.command()
    @setsyntax
    @Cli.command('UPDATE-TR-ROW trid tbname rowid')
    @argo('trid', Int, None)
    @argo('tbname', Str, None)
    @argo('rowid', Int, None)
    @argo('dicta', Dicta, {})
    def do_update_tr_row(self, trid, tbname, rowid, dicta):
        print('update-tr-row {0} {1} {2} {3}'.format(trid, tbname, rowid, dicta))
        self._db.tr_update_row_from_table(trid, tbname, rowid, **dicta)

    @Cli.command()
    @setsyntax
    @syntax('COMMIT-TR trid')
    @argo('trid', Int, None)
    def do_commit_tr(self, trid):
        print('commit-tr {0}'.format(trid))
        self._db.tr_close(trid)

    @Cli.command()
    @setsyntax
    @syntax('CANCEL-TR trid')
    @argo('trid', Int, None)
    def do_cancel_tr(self, trid):
        print('cancel-tr {0}'.format(trid))
        self._db.tr_close(trid, False)

    @Cli.command('SELECT-TABLES')
    def do_select_tables(self, *args):
        """Select all tables created in the database.
        """
        for tbname in self._db.get_all_table_names():
            print(tbname)

    @Cli.command()
    @setsyntax
    @syntax('SELECT-TABLE tbname')
    @argo('tbname', Str, None)
    def do_select_table(self, tbname):
        """Select the content for the <tbname> table.
        """
        print('select-table {0}'.format(tbname))
        for row in self._db.get_all_row_from_table(tbname):
            print(row)

    @Cli.command()
    @setsyntax
    @syntax('SELECT-ROW tbname rowid')
    @argo('tbname', Str, None)
    @argo('rowid', Int, None)
    def do_select_row(self, tbname, rowid):
        """Select a row from a table.
        """
        print('select-row {0} from table {1}'.format(rowid, tbname))
        print(self._db.get_row_from_table(tbname, rowid))

    @Cli.command()
    @setsyntax
    @syntax('DISPLAY-TABLE-FORMAT tbname')
    @argo('tbname', Str, None)
    def do_display_table_format(self, tbname):
        """Display the format for the <tbname> table.
        """
        print(self._db.get_table_by_name(tbname).Fields)

    @Cli.command('SAVE')
    def do_save(self, *args):
        """Save database.
        """
        print('save database')
        self._db.save()

    @Cli.command('LOAD')
    def do_load(self, *args):
        """Load database.
        """
        print('load database')
        self._db.load()


if __name__ == '__main__':

    cli = Dbase()
    try:
        cli.cmdloop(prompt='Dbase> ')
    except KeyboardInterrupt:
        cli._logger.display("")
        pass
