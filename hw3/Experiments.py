import Database
#from Database                import Database
import sys
sys.path.insert(0, './Utils/')
#from Utils.WorkloadGenerator import WorkloadGenerator
from WorkloadGenerator import WorkloadGenerator
#import math, random, sys
from collections import deque
from itertools import *
from Storage.File            import StorageFile
from Storage.Page            import Page
from Storage.SlottedPage import SlottedPage
#from Catalog.Schema  import DBSchema


db = Database.Database()
dataDir = '/home/cs416/datasets/tpch-sf0.001/'
wg = WorkloadGenerator()
StorageFile.defaultPageClass = SlottedPage   # Contiguous Page
pageSize = 4096                       # 4Kb
scaleFactor = 0.5                    # Half of the data
workloadMode = 5                      # Sequential Reads
wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)
