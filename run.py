import sys
sys.path.append('../jc2li')

from base import Cli
from decorators import argo, syntax, setsyntax
from argtypes import Str, Int
import loggerator
from db import DB


class Dbase(Cli):

    def __init__(self):
        super(Dbase, self).__init__()
        self._logger = loggerator.getLoggerator('Dbase')
        self._db = None

    def _argosToDicta(self, theArgos):
        dicta = {}
        for arg in theArgos:
            pname, pval = arg.split('=')
            dicta.update({pname: pval})
        return dicta

    # @Cli.command()
    # @setsyntax
    # @syntax('CREATE-TB <FIELDS> [name default]+')
    # @argo('FIELDS', Str, None)
    # @argo('name', Str, 'name')
    # @argo('default', Str, 'default')
    # def do_create_tb(self, FIELDS, name, default):
    #     """Create a new database table.
    #     """
    #     print('create-tb {0} {1} {2}'.format(FIELDS, name, default))

    @Cli.command()
    @setsyntax
    @syntax('CREATE-DBASE name')
    @argo('name', Str, None)
    def do_create_dbase(self, name):
        """Create a new database.
        """
        print ('create-dbase {0}'.format(name))
        self._db = DB(name)

    @Cli.command('CREATE-TABLE')
    @setsyntax
    @argo('tbname', Str, None)
    def do_create_table(self, tbname, lista):
        """Create a new database table in free format.
        """
        print('create-table {0} {1}'.format(tbname, lista))
        dicta = self._argosToDicta(lista)
        self._db.createTable(tbname, dicta)

    @Cli.command('CREATE-ROW')
    @setsyntax
    @argo('tbname', Str, None)
    def do_create_row(self, tbname, lista):
        """Create a new table row in free format.
        """
        print('create-row {0} {1}'.format(tbname, lista))
        dicta = self._argosToDicta(lista)
        self._db.addRowToTable(tbname, **dicta)

    @Cli.command('CREATE-TR')
    def do_create_tr(self, *args):
        """Create a new database transaction.
        """
        _id = self._db.trCreate()
        print('create-tr {0}'.format(_id))

    @Cli.command('UPDATE-ROW')
    @setsyntax
    @argo('tbname', Str, None)
    @argo('rowid', Int, None)
    def do_update_row(self, tbname, rowid, lista):
        """Update a given row from a table.
        """
        print('update-row {0} {1} {2}'.format(tbname, rowid, lista))
        dicta = self._argosToDicta(lista)
        self._db.updateRowFromTable(tbname, rowid, **dicta)

    @Cli.command('UPDATE-TR-ROW')
    @setsyntax
    @argo('trid', Int, None)
    @argo('tbname', Str, None)
    @argo('rowid', Int, None)
    def do_update_tr_row(self, trid, tbname, rowid, lista):
        print('update-tr-row {0} {1} {2} {3}'.format(trid, tbname, rowid, lista))
        dicta = self._argosToDicta(lista)
        self._db.trUpdateRowFromTable(trid, tbname, rowid, **dicta)

    @Cli.command()
    @setsyntax
    @syntax('COMMIT-TR trid')
    @argo('trid', Int, None)
    def do_commit_tr(self, trid):
        print('commit-tr {0}'.format(trid))
        self._db.trClose(trid)

    @Cli.command()
    @setsyntax
    @syntax('CANCEL-TR trid')
    @argo('trid', Int, None)
    def do_cancel_tr(self, trid):
        print('cancel-tr {0}'.format(trid))
        self._db.trClose(trid, False)

    @Cli.command('SELECT-TABLES')
    def do_select_tables(self, *args):
        """Select all tables created in the database.
        """
        for x in self._db.getAllTableNames():
            print(x)

    @Cli.command()
    @setsyntax
    @syntax('SELECT-TABLE tbname')
    @argo('tbname', Str, None)
    def do_select_table(self, tbname):
        """Select the content for the <tbname> table.
        """
        print('select-table {0}'.format(tbname))
        for row in self._db.getAllRowFromTable(tbname):
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
        print(self._db.getRowFromTable(tbname, rowid))

    @Cli.command()
    @setsyntax
    @syntax('DISPLAY-TABLE-FORMAT tbname')
    @argo('tbname', Str, None)
    def do_display_table_format(self, tbname):
        """Display the format for the <tbname> table.
        """
        print(self._db.getTableByName(tbname).Fields)

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
        cli.cmdloop(thePrompt='Dbase> ')
    except KeyboardInterrupt:
        cli._logger.display("")
        pass
