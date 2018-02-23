from jc2cli.namespace import Handler
from jc2cli.decorators import command, argo
from jc2cli.builtin.argos import Str, Int, Dicta
import jc2cli.tools.loggerator as loggerator
from jc2db.db import Db


MODULE = 'DBASE.db_commands'
logger = loggerator.getLoggerator(MODULE)
dbase = None


@command('CREATE-DBASE name')
@argo('name', Str(), None)
def do_create_dbase(name):
    """Create a new database.
    """
    global dbase
    logger.display('create-dbase {0}'.format(name))
    dbase = Db(name)
    return True


@command('CREATE-TABLE tbname [dicta]@')
@argo('tbname', Str(), None)
@argo('dicta', Dicta(), {})
def do_create_table(tbname, dicta):
    """Create a new database table in free format.
    """
    logger.display('create-table {0} {1}'.format(tbname, dicta))
    dbase.create_table(tbname, dicta)
    return True


@command('CREATE-ROW tbname [dicta]@')
@argo('tbname', Str(), None)
@argo('dicta', Dicta(), {})
def do_create_row(tbname, dicta):
    """Create a new table row in free format.
    """
    logger.display('create-row {0} {1}'.format(tbname, dicta))
    dbase.add_row_to_table(tbname, **dicta)
    return True


@command('CREATE-TR')
def do_create_tr(*args):
    """Create a new database transaction.
    """
    _id = dbase.tr_create()
    logger.display('create-tr {0}'.format(_id))
    return True


@command('UPDATE-ROW tbname rowid [dicta]@')
@argo('tbname', Str(), None)
@argo('rowid', Int(), None)
@argo('dicta', Dicta(), {})
def do_update_row(tbname, rowid, dicta):
    """Update a given row from a table.
    """
    logger.display('update-row {0} {1} {2}'.format(tbname, rowid, dicta))
    dbase.update_row_from_table(tbname, rowid, **dicta)
    return True


@command('UPDATE-TR-ROW trid tbname rowid [dicta]@')
@argo('trid', Int(), None)
@argo('tbname', Str(), None)
@argo('rowid', Int(), None)
@argo('dicta', Dicta(), {})
def do_update_tr_row(trid, tbname, rowid, dicta):
    logger.display('update-tr-row {0} {1} {2} {3}'.format(trid, tbname, rowid, dicta))
    dbase.tr_update_row_from_table(trid, tbname, rowid, **dicta)
    return True


@command('COMMIT-TR trid')
@argo('trid', Int(), None)
def do_commit_tr(trid):
    logger.display('commit-tr {0}'.format(trid))
    dbase.tr_close(trid)
    return True


@command('CANCEL-TR trid')
@argo('trid', Int(), None)
def do_cancel_tr(trid):
    logger.display('cancel-tr {0}'.format(trid))
    dbase.tr_close(trid, False)
    return True


@command('SELECT-TABLES')
def do_select_tables(*args):
    """Select all tables created in the database.
    """
    for tbname in dbase.get_all_table_names():
        logger.display(tbname)
    return True


@command('SELECT-TABLE tbname')
@argo('tbname', Str(), None)
def do_select_table(tbname):
    """Select the content for the <tbname> table.
    """
    logger.display('select-table {0}'.format(tbname))
    for row in dbase.get_all_row_from_table(tbname):
        logger.display(row)
    return True


@command('SELECT-ROW tbname rowid')
@argo('tbname', Str(), None)
@argo('rowid', Int(), None)
def do_select_row(tbname, rowid):
    """Select a row from a table.
    """
    logger.display('select-row {0} from table {1}'.format(rowid, tbname))
    logger.display(dbase.get_row_from_table(tbname, rowid))
    return True


@command('DISPLAY-TABLE-FORMAT tbname')
@argo('tbname', Str(), None)
def do_display_table_format(tbname):
    """Display the format for the <tbname> table.
    """
    logger.display(dbase.get_table_by_name(tbname).fields)
    return True


@command('SAVE')
def do_save(*args):
    """Save database.
    """
    logger.display('save database')
    dbase.save()
    return True


@command('LOAD')
def do_load(*args):
    """Load database.
    """
    logger.display('load database')
    dbase.load()
    return True


if __name__ == '__main__':
    h = Handler()
    h.create_namespace('__main__')
    h.switch_and_run_namespace('__main__')
