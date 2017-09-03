import sys


sys.path.insert(0, 'jc2li.zip')
import jc2li.loggerator as loggerator
logger = loggerator.getLoggerator('main')
logger.info('MAIN APPLICATION')
