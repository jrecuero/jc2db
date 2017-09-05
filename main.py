import sys
import db


sys.path.insert(0, 'jc2li.zip')
import jc2li.loggerator as loggerator
logger = loggerator.getLoggerator('main')
logger.info('MAIN APPLICATION')


def person(db, table, row):
    otherTable = db.getTableByName('Company')
    for otherRow in otherTable.Table:
        if otherRow.NAME == row.company:
            row.active = True
    otherTable.registerLink('NAME', row.company, table.Name, row.id)


def company(db, table, row):
    for link in table.RegisterLinks:
        if getattr(row, link._field) == link._value:
            logger.display('there is a link to table {0} row {1}'.format(link._linkTable, link._linkRow))
            db.getRowFromTable(link._linkTable, link._linkRow).update(active=True)


myDB = db.DB()
myDB.createTable('Person', {'name': '---', 'age': 0, 'company': '---', 'active': False})
myDB.createTable('Company', {'NAME': '***', 'COUNTRY': '***'})
myDB.getTableByName('Person').RowCb.append(person)
myDB.getTableByName('Company').RowCb.append(company)

jc = myDB.addRowToTable('Person', name='Jose Carlos', age=50, company='CISCO')
logger.display(jc)
matias = myDB.addRowToTable('Person', name='Matias', age=50, company='CISCO')
logger.display(matias)
joselu = myDB.addRowToTable('Person', name='Joselu', age=50, company='NOKIA')
logger.display(joselu)
cisco = myDB.addRowToTable('Company', NAME='CISCO', COUNTRY='USA')
logger.display(cisco)
_jc = myDB.getRowFromTable('Person', jc.id)
_matias = myDB.getRowFromTable('Person', matias.id)
_joselu = myDB.getRowFromTable('Person', joselu.id)
logger.display(_jc)
logger.display(_matias)
logger.display(_joselu)
