import io, math, os, os.path, random, shutil, time, timeit,sys

from Catalog.Schema        import DBSchema
from Storage.StorageEngine import StorageEngine
from Database              import Database

class CSVParser:
  def __init__(self, separator, fieldParsers):
    self.separator = separator
    self.fieldParsers = fieldParsers

  def parse(self, line):
    fields = line.split(self.separator)
    return map(lambda x: (x[0])(x[1]), zip(self.fieldParsers, fields))


class WorkloadGenerator:
  """
  A workload generator for random read operations.

  >>> wg = WorkloadGenerator()
  >>> db = Database()

  >>> wg.parseDate('1996-01-01')
  19960101

  >>> wg.createRelations(db)
  >>> sorted(list(db.relations()))
  ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']

  >>> wg.loadDataset(db, 'test/datasets/tpch-tiny', 1.0)
  >>> [wg.schemas['nation'].unpack(t).N_NATIONKEY for t in db.storageEngine().tuples('nation')]
  [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24]

  >>> wg.loadDataset(db, 'test/datasets/tpch-tiny', 1.0)
  >>> [wg.schemas['orders'].unpack(t).O_ORDERKEY for t in db.storageEngine().tuples('orders')] # doctest:+ELLIPSIS
  [1, 2, 3, ..., 582]

  >>> db.close()
  >>> shutil.rmtree(db.fileManager().dataDir, ignore_errors=True)
  >>> del db

  >>> wg.runWorkload('test/datasets/tpch-tiny', 1.0, 4096, 1) # doctest:+ELLIPSIS
  Tuples: 736
  Throughput: ...
  Execution time: ...

  >>> wg.runWorkload('test/datasets/tpch-tiny', 1.0, 4096, 2) # doctest:+ELLIPSIS
  Tuples: 736
  Throughput: ...
  Execution time: ...

  >>> wg.runWorkload('test/datasets/tpch-tiny', 1.0, 4096, 3) # doctest:+ELLIPSIS
  Tuples: 736
  Throughput: ...
  Execution time: ...

  >>> wg.runWorkload('test/datasets/tpch-tiny', 1.0, 4096, 4) # doctest:+ELLIPSIS
  Tuples: 736
  Throughput: ...
  Execution time: ...

  >>> print("Total time: " + str( \
            timeit.timeit(stmt="wg = WorkloadGenerator(); wg.runWorkload('test/datasets/tpch-tiny', 1.0, 4096, 1)", \
                          setup="from __main__ import WorkloadGenerator", number=10))) # doctest:+ELLIPSIS
  Tuples: ...
  Total time: ...
  """

  def __init__(self):
    random.seed(a=12345)
    self.initializeSchemas()

  # Create schemas for the TPC-H dataset
  def initializeSchemas(self):
    tpchNamesAndFields = [
        ('part',     [ ('P_PARTKEY'    , 'int'),
                       ('P_NAME'       , 'char(55)'),
                       ('P_MFGR'       , 'char(25)'),
                       ('P_BRAND'      , 'char(10)'),
                       ('P_TYPE'       , 'char(25)'),
                       ('P_SIZE'       , 'int'),
                       ('P_CONTAINER'  , 'char(10)'),
                       ('P_RETAILPRICE', 'double'),
                       ('P_COMMENT'    , 'char(23)') ]
               ,      "issssisds"),

        ('supplier', [ ('S_SUPPKEY'   , 'int'),
                       ('S_NAME'      , 'char(25)'),
                       ('S_ADDRESS'   , 'char(40)'),
                       ('S_NATIONKEY' , 'int'),
                       ('S_PHONE'     , 'char(15)'),
                       ('S_ACCTBAL'   , 'double'),
                       ('S_COMMENT'   , 'char(101)') ]
                   ,  "issisds"),

        ('partsupp', [ ('PS_PARTKEY'    , 'int'),
                       ('PS_SUPPKEY'    , 'int'),
                       ('PS_AVAILQTY'   , 'int'),
                       ('PS_SUPPLYCOST' , 'double'),
                       ('PS_COMMENT'    , 'char(199)') ]
                   , "iiids"),

        ('customer', [ ('C_CUSTKEY'    , 'int'),
                       ('C_NAME'       , 'char(25)'),
                       ('C_ADDRESS'    , 'char(40)'),
                       ('C_NATIONKEY'  , 'int'),
                       ('C_PHONE'      , 'char(15)'),
                       ('C_ACCTBAL'    , 'double'),
                       ('C_MKTSEGMENT' , 'char(10)'),
                       ('C_COMMENT'    , 'char(117)') ]
                   , "issisdss"),

        ('orders',   [ ('O_ORDERKEY'      , 'int'),
                       ('O_CUSTKEY'       , 'int'),
                       ('O_ORDERSTATUS'   , 'char(1)'),
                       ('O_TOTALPRICE'    , 'double'),
                       ('O_ORDERDATE'     , 'int'),  # date
                       ('O_ORDERPRIORITY' , 'char(15)'),
                       ('O_CLERK'         , 'char(15)'),
                       ('O_SHIPPRIORITY'  , 'int'),
                       ('O_COMMENT'       , 'char(79)') ]
                 ,   "iisdtssis"),

        ('lineitem', [ ('L_ORDERKEY'      , 'int'),
                       ('L_PARTKEY'       , 'int'),
                       ('L_SUPPKEY'       , 'int'),
                       ('L_LINENUMBER'    , 'int'),
                       ('L_QUANTITY'      , 'double'),
                       ('L_EXTENDEDPRICE' , 'double'),
                       ('L_DISCOUNT'      , 'double'),
                       ('L_TAX'           , 'double'),
                       ('L_RETURNFLAG'    , 'char(1)'),
                       ('L_LINESTATUS'    , 'char(1)'),
                       ('L_SHIPDATE'      , 'int'),   # date
                       ('L_COMMITDATE'    , 'int'),   # date
                       ('L_RECEIPTDATE'   , 'int'),   # date
                       ('L_SHIPINSTRUCT'  , 'char(25)'),
                       ('L_SHIPMODE'      , 'char(10)'),
                       ('L_COMMENT'       , 'char(44)') ]
                   , "iiiiddddsstttsss"),

        ('nation',   [ ('N_NATIONKEY'  , 'int'),
                       ('N_NAME'       , 'char(25)'),
                       ('N_REGIONKEY'  , 'int'),
                       ('N_COMMENT'    , 'char(152)') ]
                 ,   "isis"),

        ('region',   [ ('R_REGIONKEY' , 'int'),
                       ('R_NAME'      , 'char(25)'),
                       ('R_COMMENT'   , 'char(152)') ]
                 ,   "iss")
      ]

    self.schemas = dict(map(lambda x: (x[0], DBSchema(x[0], x[1])), tpchNamesAndFields))
    self.parsers = dict(map(lambda x: (x[0], self.buildParser(x[2])), tpchNamesAndFields))

  # Dates are represented as integers, e.g., 1996-01-01 becomes 19960101
  def parseDate(self, dateStr):
    (year, month, day) = dateStr.split('-')
    return int(year) * 10000 + int(month) * 100 + int(day)

  # Build a CSV parser object for a given format string.
  # Format strings may include: 'i' (int), 'd' (double), 's' (string), 't' (date, converted to int).
  def buildParser(self, fmtStr):
    fieldParsers = []
    for i in fmtStr:
      if i == 'i':
        fieldParsers.append(lambda x: int(x))
      elif i == 'd':
        fieldParsers.append(lambda x: float(x))
      elif i == 's':
        fieldParsers.append(lambda x: x)
      elif i == 't':
        fieldParsers.append(lambda x: self.parseDate(x))
      else:
        raise ValueError("Invalid TPC-H type")

    return CSVParser("|", fieldParsers)

  # Create the TPC-H relations in the given storage engine, removing if already present.
  def createRelations(self, db):
    for i in self.schemas:
      if db.hasRelation(i):
        db.removeRelation(i)
      db.createRelation(i, self.schemas[i].schema())

  # Load the CSV files corresponding to the TPC-H relations into the given storage engine.
  # This method (naively) samples the dataset based on the scale factor.
  def loadDataset(self, db, datadir, scaleFactor):
    self.tupleIds = {}
    for i in self.schemas:
      if db.hasRelation(i):
        filePath = os.path.join(datadir, i+".csv")
        if os.path.exists(filePath):
          with open(filePath) as f:
            self.tupleIds[i] = []
            for line in f:
              if random.random() <= scaleFactor:
                tup = self.schemas[i].instantiate(*(self.parsers[i].parse(line)))
                tupleId = db.insertTuple(i, self.schemas[i].pack(tup))
                if tupleId is not None:
                  self.tupleIds[i].append(tupleId)
                else:
                  raise ValueError("Failed to insert tuple")
        else:
          raise ValueError("Could not find file: " + filePath)
      else:
        raise ValueError("Uninitialized relation: "+i)

  # Scan through all the stored tuples for the given relations
  def scanRelations(self, db, relations):
    start = time.time()
    tuplesRead = 0

    # Sequentially read through relations
    for rel in relations:
      for t in db.storageEngine().tuples(rel):
        tuplesRead += 1

    end = time.time()
    print("Tuples: " + str(tuplesRead))
    print("Throughput: " + str(tuplesRead / (end - start)))
    print("Execution time: " + str(end - start))

  # Randomized access for 1/fraction read operations on the
  # stored tuples for the given relations.
  def randomizedOperations(self, db, relations, fraction):

    # Build a dict of random operations. When encountering the dict key,
    # perform a read operation on the tuple id at the dict value.
    randomOperations = {}
    for r in relations:
      sampleSize = math.floor(len(self.tupleIds[r]) * fraction)
      randomOperations[r] = \
        dict(zip(random.sample(self.tupleIds[r], sampleSize), \
                 random.sample(self.tupleIds[r], sampleSize)))

    tuplesRead = 0
    start = time.time()

    # Read tuples w/ random operations.
    for r in relations:
      for tupleId in self.tupleIds[r]:
        if tupleId in randomOperations[r]:
          realTupleId = randomOperations[r][tupleId]
          pId = realTupleId.pageId
        else:
          realTupleId = tupleId
          pId = tupleId.pageId

        page = db.bufferPool().getPage(pId)
        if page.getTuple(realTupleId):
          tuplesRead += 1

    end = time.time()
    print("Tuples: " + str(tuplesRead))
    print("Throughput: " + str(tuplesRead / (end - start)))
    print("Execution time: " + str(end - start))

  def query1(self,db,relations,method):
      if(method == 'block-nested-loops'):
        start = time.time()
        #raise NotImplementedError
        #print(self.schemas)

        #SELECT 1
        query1 = db.query().fromTable('partsupp').where("PS_AVAILQTY == 1")

        #JOIN 1
        query1 = query1.join(db.query().fromTable('supplier'),\
        rhsSchema=self.schemas['supplier'],\
        method=method, expr='PS_SUPPKEY == S_SUPPKEY')

        #JOIN 2
        query1 = query1.join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='PS_PARTKEY == P_PARTKEY')

        #SELECT 2
        query2 = db.query().fromTable('partsupp').where("PS_SUPPLYCOST < 5")

        #JOIN 1
        query2 = query2.join(db.query().fromTable('supplier'),\
        rhsSchema=self.schemas['supplier'],\
        method=method, expr='PS_SUPPKEY == S_SUPPKEY')

        #JOIN 2
        query2 = query2.join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='PS_PARTKEY == P_PARTKEY')

        #UNION
        query3 = query1.union(query2).finalize()

        end = time.time()
        print("query1 BNLJ")
        print("Execution time: " + str(end - start))
      else:
        start = time.time()
        #raise NotImplementedError
        #print(self.schemas)

        #SELECT 1
        query1 = db.query().fromTable('partsupp').where("PS_AVAILQTY == 1")

        #JOIN 1
        keySchema  = DBSchema('suppkey1',  [('PS_SUPPKEY', 'int')])
        keySchema2 = DBSchema('suppkey2', [('S_SUPPKEY', 'int')])
        query1 = query1.join(db.query().fromTable('supplier'),\
        rhsSchema=self.schemas['supplier'],\
        method=method, expr='PS_SUPPKEY == S_SUPPKEY',
        lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2)

        #JOIN 2
        keySchema  = DBSchema('partkey1',  [('PS_PARTKEY', 'int')])
        keySchema2 = DBSchema('partkey2', [('P_PARTKEY', 'int')])
        query1 = query1.join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='PS_PARTKEY == P_PARTKEY',
        lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2)

        #SELECT 2
        query2 = db.query().fromTable('partsupp').where("PS_SUPPLYCOST < 5")

        #JOIN 2
        keySchema  = DBSchema('suppkey1',  [('PS_SUPPKEY', 'int')])
        keySchema2 = DBSchema('suppkey2', [('S_SUPPKEY', 'int')])
        query2 = query2.join(db.query().fromTable('supplier'),\
        rhsSchema=self.schemas['supplier'],\
        method=method, expr='PS_SUPPKEY == S_SUPPKEY',
        lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2)

        #JOIN 2
        keySchema  = DBSchema('partkey1',  [('PS_PARTKEY', 'int')])
        keySchema2 = DBSchema('partkey2', [('P_PARTKEY', 'int')])
        query2 = query2.join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='PS_PARTKEY == P_PARTKEY',
        lhsHashFn='hash(id) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(id2) % 4', rhsKeySchema=keySchema2)

        #UNION
        query3 = query1.union(query2).finalize()

        end = time.time()
        print("query1 HASH")
        print("Execution time: " + str(end - start))




  def query2(self,db,relations,method):
      if(method == 'block-nested-loops'):
        start = time.time()
        #raise NotImplementedError

        #SELECT 1
        query1 = db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R' ")

        #JOIN 1
        query1 = query1.join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='L_PARTKEY == P_PARTKEY')

        #query1 = query1.select({'ID':('P_PARTKEY'),'P_NAME': ('P_NAME', 'char(55)')})

        #GROUP BY
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('P_NAME',[('P_NAME','char(55)')])#,[('count','int')])
        #print(len(nameCountSchema.fields))
        #print(len([(1,2,3)]))
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.P_NAME), \
          aggExprs=[("part", lambda acc, e: e.P_NAME, lambda x: x)] , \
          groupHashFn=(lambda gbVal: hash(gbVal) % 1000000))

        #SELECT 2
        query1 = query1.select({'P_NAME': ('P_NAME', 'char(55)')}).finalize()

        #COUNT
        #'''
        results = [nameCountSchema.unpack(tup) for page in db.processQuery(query1) for tup in page[1] ]

        dict1 = {}
        for tup in results:
            if(tup[0] in dict1):
                dict1[tup[0]] = dict1[tup[0]] + 1
            else:
                dict1[tup[0]] = 1
        #'''

        print("query2 BNLJ")
        end = time.time()
        print("Execution time: " + str(end - start))
      else:
        start = time.time()
        #raise NotImplementedError

        #SELECT 1
        query1 = db.query().fromTable('lineitem').where("L_RETURNFLAG == 'R' ")

        #JOIN 1
        keySchema  = DBSchema('partkey1',  [('L_PARTKEY', 'int')])
        keySchema2 = DBSchema('partkey2', [('P_PARTKEY', 'int')])
        query1 = query1.join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='L_PARTKEY == P_PARTKEY',
        lhsHashFn='hash(L_PARTKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(P_PARTKEY) % 4', rhsKeySchema=keySchema2)

        #GROUP BY
        # hash by name
        # relation by name
        #print out by name
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('P_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.P_NAME), \
          aggExprs=[("part", lambda acc, e: e.P_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 1000) \
        )

        #SELECT 2
        query1 = query1.select({'P_NAME': ('P_NAME', 'char(55)')}).finalize()

        #COUNT
        #'''
        results = [nameSchema.unpack(tup) for page in db.processQuery(query1) for tup in page[1]]
        dict1 = {}
        for tup in results:
            if(tup[0] in dict1):
                dict1[tup[0]] = dict1[tup[0]] + 1
            else:
                dict1[tup[0]] = 1

        #'''
        print("query2 HASH")
        end = time.time()
        print("Execution time: " + str(end - start))


  def query3(self,db,relations,method):
      if(method == 'block-nested-loops'):
        start = time.time()
        #raise NotImplementedError

        #JOIN 1
        query1 = db.query().fromTable('lineitem').join(db.query().fromTable('orders'),\
        rhsSchema=self.schemas['orders'],\
        method=method, expr='L_ORDERKEY == O_ORDERKEY')

        #JOIN 2
        query1 = query1.join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='L_PARTKEY == P_PARTKEY')

        #JOIN 3
        query1 = query1.join(db.query().fromTable('customer'),\
        rhsSchema=self.schemas['customer'],\
        method=method, expr='O_CUSTKEY == C_CUSTKEY')

        #JOIN 4
        query1 = query1.join(db.query().fromTable('nation'),\
        rhsSchema=self.schemas['nation'],\
        method=method, expr='C_NATIONKEY == N_NATIONKEY')

        #GROUPY BY 1
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('P_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.P_NAME), \
          aggExprs=[("part", lambda acc, e: e.P_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #GROUP BY 2
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('P_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.N_NAME), \
          aggExprs=[("part", lambda acc, e: e.N_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #SELECT 1
        query1 = query1.select({'nation': ('N_NAME', 'char(55)')})
        query1 = query1.select({'part':('P_NAME','char(55)')})#.finalize()

        #GROUP BY 3
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('N_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.N_NAME), \
          aggExprs=[("part", lambda acc, e: e.N_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #SELECT 2
        query1 = query1.select({'nation': ('N_NAME', 'char(55)')}).finalize()

        #SUM
        #'''
        results = [nameSchema.unpack(tup) for page in db.processQuery(query1) for tup in page[1] ]
        dict1 = {}
        for tup in results:
            if(tup[0] in dict1):
                dict1[tup[0]] = dict1[tup[0]] + 1
            else:
                dict1[tup[0]] = 1
        #'''

        print("query3 BNLJ")
        end = time.time()
        print("Execution time: " + str(end - start))
      else:
        start = time.time()
        #raise NotImplementedError

        #JOIN 1
        keySchema  = DBSchema('orderkey1',  [('L_ORDERKEY', 'int')])
        keySchema2 = DBSchema('orderkey2', [('O_ORDERKEY', 'int')])
        query1 = db.query().fromTable('lineitem').join(db.query().fromTable('orders'),\
        rhsSchema=self.schemas['orders'],\
        method=method, expr='L_ORDERKEY == O_ORDERKEY',
        lhsHashFn='hash(L_ORDERKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(O_ORDERKEY) % 4', rhsKeySchema=keySchema2)

        #JOIN 2
        keySchema  = DBSchema('partkey1',  [('L_PARTKEY', 'int')])
        keySchema2 = DBSchema('partkey2', [('P_PARTKEY', 'int')])
        query1 = query1.join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='L_PARTKEY == P_PARTKEY',
        lhsHashFn='hash(L_PARTKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(P_PARTKEY) % 4', rhsKeySchema=keySchema2)

        #JOIN 3
        keySchema  = DBSchema('custkey1',  [('O_CUSTKEY', 'int')])
        keySchema2 = DBSchema('custkey2', [('C_CUSTKEY', 'int')])
        query1 = query1.join(db.query().fromTable('customer'),\
        rhsSchema=self.schemas['customer'],\
        method=method, expr='O_CUSTKEY == C_CUSTKEY',
        lhsHashFn='hash(O_CUSTKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(C_CUSTKEY) % 4', rhsKeySchema=keySchema2)

        #JOIN 4
        keySchema  = DBSchema('nationkey1',  [('C_NATIONKEY', 'int')])
        keySchema2 = DBSchema('nationkey2', [('N_NATIONKEY', 'int')])
        query1 = query1.join(db.query().fromTable('nation'),\
        rhsSchema=self.schemas['nation'],\
        method=method, expr='C_NATIONKEY == N_NATIONKEY',
        lhsHashFn='hash(C_NATIONKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(N_NATIONKEY) % 4', rhsKeySchema=keySchema2)

        #GROUPY BY 1
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('P_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.P_NAME), \
          aggExprs=[("part", lambda acc, e: e.P_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #GROUP BY 2
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('N_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.N_NAME), \
          aggExprs=[("part", lambda acc, e: e.N_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #SELECT 1
        query1 = query1.select({'nation': ('N_NAME', 'char(55)')})
        query1 = query1.select({'part':('P_NAME','char(55)')})#.finalize()

        #GROUP BY 3
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('N_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.N_NAME), \
          aggExprs=[("part", lambda acc, e: e.N_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #SELECT 2
        query1 = query1.select({'nation': ('N_NAME', 'char(55)')}).finalize()

        #SUM
        #'''
        results = [nameSchema.unpack(tup) for page in db.processQuery(query1) for tup in page[1] ]
        dict1 = {}
        for tup in results:
            if(tup[0] in dict1):
                dict1[tup[0]] = dict1[tup[0]] + 1
            else:
                dict1[tup[0]] = 1
        #'''

        print("query3 HASH")
        end = time.time()
        print("Execution time: " + str(end - start))

  def query4(self,db,relations,method):
      if(method == 'block-nested-loops'):
        start = time.time()
        #raise NotImplementedError

        #JOIN 1
        query1 = db.query().fromTable('lineitem').join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='L_PARTKEY == P_PARTKEY')

        #JOIN 2
        query1 = query1.join(db.query().fromTable('orders'),\
        rhsSchema=self.schemas['orders'],\
        method=method, expr='L_ORDERKEY == O_ORDERKEY')

        #JOIN 3
        query1 = query1.join(db.query().fromTable('customer'),\
        rhsSchema=self.schemas['customer'],\
        method=method, expr='O_CUSTKEY == C_CUSTKEY')

        #JOIN 4
        query1 = query1.join(db.query().fromTable('nation'),\
        rhsSchema=self.schemas['nation'],\
        method=method, expr='C_NATIONKEY == N_NATIONKEY')

        #GROUPY BY 1
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('P_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.P_NAME), \
          aggExprs=[("part", lambda acc, e: e.P_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #GROUP BY 2
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('N_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.N_NAME), \
          aggExprs=[("part", lambda acc, e: e.N_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #SELECT 1
        query1 = query1.select({'nation': ('N_NAME', 'char(55)')})
        query1 = query1.select({'part':('P_NAME','char(55)')})#.finalize()

        #GROUP BY 3
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('N_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.N_NAME), \
          aggExprs=[("part", lambda acc, e: e.N_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #SELECT 2
        query1 = query1.select({'nation': ('N_NAME', 'char(55)')}).finalize()

        #SUM
        #'''
        results = [nameSchema.unpack(tup) for page in db.processQuery(query1) for tup in page[1] ]
        dict1 = {}
        for tup in results:
            if(tup[0] in dict1):
                dict1[tup[0]] = dict1[tup[0]] + 1
            else:
                dict1[tup[0]] = 1
        #'''

        print("query4 BNLJ")
        end = time.time()
        print("Execution time: " + str(end - start))
      else:
        start = time.time()
        #raise NotImplementedError

        #JOIN 1
        keySchema  = DBSchema('partkey1',  [('L_PARTKEY', 'int')])
        keySchema2 = DBSchema('partkey2', [('P_PARTKEY', 'int')])
        query1 = db.query().fromTable('lineitem').join(db.query().fromTable('part'),\
        rhsSchema=self.schemas['part'],\
        method=method, expr='L_PARTKEY == P_PARTKEY',
        lhsHashFn='hash(L_PARTKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(P_PARTKEY) % 4', rhsKeySchema=keySchema2)

        #JOIN 2
        keySchema  = DBSchema('orderkey1',  [('L_ORDERKEY', 'int')])
        keySchema2 = DBSchema('orderkey2', [('O_ORDERKEY', 'int')])
        query1 = query1.join(db.query().fromTable('orders'),\
        rhsSchema=self.schemas['orders'],\
        method=method, expr='L_ORDERKEY == O_ORDERKEY',
        lhsHashFn='hash(L_ORDERKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(O_ORDERKEY) % 4', rhsKeySchema=keySchema2)

        #JOIN 3
        keySchema  = DBSchema('custkey1',  [('O_CUSTKEY', 'int')])
        keySchema2 = DBSchema('custkey2', [('C_CUSTKEY', 'int')])
        query1 = query1.join(db.query().fromTable('customer'),\
        rhsSchema=self.schemas['customer'],\
        method=method, expr='O_CUSTKEY == C_CUSTKEY',
        lhsHashFn='hash(O_CUSTKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(C_CUSTKEY) % 4', rhsKeySchema=keySchema2)

        #JOIN 4
        keySchema  = DBSchema('nationkey1',  [('C_NATIONKEY', 'int')])
        keySchema2 = DBSchema('nationkey2', [('N_NATIONKEY', 'int')])
        query1 = query1.join(db.query().fromTable('nation'),\
        rhsSchema=self.schemas['nation'],\
        method=method, expr='C_NATIONKEY == N_NATIONKEY',
        lhsHashFn='hash(C_NATIONKEY) % 4',  lhsKeySchema=keySchema, \
          rhsHashFn='hash(N_NATIONKEY) % 4', rhsKeySchema=keySchema2)

        #GROUPY BY 1
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('P_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.P_NAME), \
          aggExprs=[("part", lambda acc, e: e.P_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #GROUP BY 2
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('N_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.N_NAME), \
          aggExprs=[("part", lambda acc, e: e.N_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #SELECT 1
        query1 = query1.select({'nation': ('N_NAME', 'char(55)')})
        query1 = query1.select({'part':('P_NAME','char(55)')})#.finalize()

        #GROUP BY 3
        nameSchema = DBSchema('id', [('id', 'int')])
        nameCountSchema = DBSchema('namecount',[('N_NAME','char(55)')])#,[('count','int')])
        query1 = query1.groupBy( \
          groupSchema=nameSchema, \
          aggSchema=nameCountSchema, \
          groupExpr=(lambda e: e.N_NAME), \
          aggExprs=[("part", lambda acc, e: e.N_NAME, lambda x: x)], \
          groupHashFn=(lambda gbVal: hash(gbVal) % 100) \
        )

        #SELECT 2
        query1 = query1.select({'nation': ('N_NAME', 'char(55)')}).finalize()

        #SUM
        #'''
        results = [nameSchema.unpack(tup) for page in db.processQuery(query1) for tup in page[1] ]
        dict1 = {}
        for tup in results:
            if(tup[0] in dict1):
                dict1[tup[0]] = dict1[tup[0]] + 1
            else:
                dict1[tup[0]] = 1
        #'''

        print("query4 HASH")
        end = time.time()
        print("Execution time: " + str(end - start))

  # Dispatch a workload mode.
  def runOperations(self, db, mode):
    if hasattr(self, 'tupleIds') and self.tupleIds:
      if mode == 1:
        self.scanRelations(db, ['lineitem', 'orders'])

      elif mode == 2:
        self.randomizedOperations(db, ['lineitem', 'orders'], 0.2)

      elif mode == 3:
        self.randomizedOperations(db, ['lineitem', 'orders'], 0.5)

      elif mode == 4:
        self.randomizedOperations(db, ['lineitem', 'orders'], 0.8)

      elif mode == 5:
        self.query1(db,['part','supplier','partsupp'],"block-nested-loops")
        self.query1(db,['part','supplier','partsupp'],"hash")

      elif mode == 6:
        self.query2(db,['part','lineitem'],"block-nested-loops")
        self.query2(db,['part','lineitem'],"hash")

      elif mode == 7:
        self.query3(db,['nation','part','lineitem','customer','orders'],"block-nested-loops")
        self.query3(db,['nation','part','lineitem','customer','orders'],"hash")

      elif mode == 8:
        self.query4(db,['nation','part','lineitem','customer','orders'],"block-nested-loops")
        self.query4(db,['nation','part','lineitem','customer','orders'],"hash")

      else:
        raise ValueError("Invalid workload mode (expected 1-4): "+str(mode))
    else:
      raise ValueError("No tuple ids found, has the dataset been loaded?")

  def runWorkload(self, datadir, scaleFactor, pageSize, workloadMode):
    db = Database(pageSize=pageSize)
    self.createRelations(db)
    #print(time.time())
    self.loadDataset(db, datadir, scaleFactor)
    #print(time.time())
    self.runOperations(db, workloadMode)
    self.runOperations(db, workloadMode + 1)
    self.runOperations(db, workloadMode + 2)
    self.runOperations(db, workloadMode + 3)
    db.close()
    shutil.rmtree(db.fileManager().dataDir, ignore_errors=True)
    del db

if __name__ == "__main__":
    import doctest
    doctest.testmod()
