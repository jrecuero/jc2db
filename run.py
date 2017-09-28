import sys
sys.path.append('../jc2li')

import shlex
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
    def do_create_table(self, *args):
        """Create a new database table in free format.
        """
        print('create-table {0}'.format(shlex.split(args[0])))
        argos = shlex.split(args[0])
        tbName = argos[0]
        dicta = self._argosToDicta(argos[1:])
        self._db.createTable(tbName, dicta)

    @Cli.command('CREATE-ROW')
    def do_create_row(self, *args):
        """Create a new table row in free format.
        """
        print('create-row {0}'.format(shlex.split(args[0])))
        argos = shlex.split(args[0])
        tbName = argos[0]
        dicta = self._argosToDicta(argos[1:])
        self._db.addRowToTable(tbName, **dicta)

    @Cli.command('UPDATE-ROW')
    def do_update_row(self, *args):
        """Update a given row from a table.
        """
        print('update-row {0}'.format(shlex.split(args[0])))
        argos = shlex.split(args[0])
        tbName = argos[0]
        rowId = int(argos[1])
        dicta = self._argosToDicta(argos[2:])
        self._db.updateRowFromTable(tbName, rowId, **dicta)

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
