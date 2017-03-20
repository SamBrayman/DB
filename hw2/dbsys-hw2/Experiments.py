import Database
from Utils.WorkloadGenerator import WorkloadGenerator
import math, random, sys
from collections import deque
from itertools import *
from Catalog.Schema  import DBSchema


db = Database.Database()
dataDir = '/home/cs416/datasets/tpch-sf0.01/'
wg = WorkloadGenerator()
StorageFile.defaultPageClass = SlottedPage   # Contiguous Page
pageSize = 4096                       # 4Kb
scaleFactor = 0.1                     # Half of the data
workloadMode = 5                      # Sequential Reads
wg.runWorkload(dataDir, scaleFactor, pageSize, workloadMode)
