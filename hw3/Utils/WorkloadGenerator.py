import io, math, os, os.path, random, shutil, time, timeit

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


  def query1(self,db,relations,opt):
      start = time.time()
      '''query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid').finalize()
        .select({'id': ('id', 'int')})
'''
      query = db.query().fromTable(relations[0]).where("L_SHIPDATE >= 19940101")
      query = query.where("L_SHIPDATE < 19950101")
      query = query.where("L_DISCOUNT > .05")
      query = query.where("L_DISCOUNT < .07")
      query = query.where("L_QUANTITY < 24")
      #sum(l_extendedprice * l_discount) as revenue
      query = query.select({'L_EXTENDEDPRICE': ('L_EXTENDEDPRICE','double'), 'L_DISCOUNT':('L_DISCOUNT','double')})
      query = query.finalize()
      nameSchema = DBSchema('atts', [('L_EXTENDEDPRICE','double'),('L_DISCOUNT','double')])
      if(opt):
          db.optimizer.pushdownOperators(query)
          query = db.optimizer.pickJoinOrder(query)
          results = [nameSchema.unpack(tup) for page in db.processQuery(query) for tup in page[1] ]
      else:
          results = [nameSchema.unpack(tup) for page in db.processQuery(query) for tup in page[1] ]
          #print(results)
      sumLEPLD = 0.0
      for tup in results:
          sumLEPLD += tup[0] * tup[1]
      end = time.time()
      if(opt):
        print("query1 Optimized")
      else:
        print("query1")
      print("Execution time: " + str(end - start))


  def query2(self,db,relations,opt):
      start = time.time()
      query = db.query().fromTable(relations[0]).where("L_SHIPDATE >= 19950901")
      query = query.where("L_SHIPDATE < 19951001")
      #query2 = db.query().fromTable(relations[1])
      query = query.join( \
              db.query().fromTable(relations[1]),\
              method='block-nested-loops', expr = 'L_PARTKEY == P_PARTKEY')
      query = query.select({'L_EXTENDEDPRICE': ('L_EXTENDEDPRICE','double'), 'L_DISCOUNT':('L_DISCOUNT','double')})
      query = query.finalize()
      #query2.finalize()
      nameSchema = DBSchema('atts', [('L_EXTENDEDPRICE','double'),('L_DISCOUNT','double')])
      #print(query.explain())
      if(opt):
          db.optimizer.pushdownOperators(query)
          #print(query)
          query = db.optimizer.pickJoinOrder(query)
          #print(query)
          #print(query.explain())
          #print(query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1])
          results = [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1] ]
      else:
          results = [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1] ]
          #print(results)
      sumLEPLD = 0.0
      for tup in results:
          sumLEPLD += tup[0] * (1-tup[1])
      end = time.time()
      if(opt):
        print("query2 Optimized")
      else:
        print("query2")
      print("Execution time: " + str(end - start))


  def query3(self,db,relations,opt):
      start = time.time()
      query = db.query().fromTable(relations[2]).where('L_SHIPDATE > 19950315')
      query = query.join( \
              db.query().fromTable(relations[1]).where('O_ORDERDATE < 19950315'),\
              method ='block-nested-loops', expr = 'L_ORDERKEY == O_ORDERKEY')
      query = query.join( \
              db.query().fromTable(relations[0]).where('C_MKTSEGMENT == \'BUILDING\''),\
              method = 'block-nested-loops',expr = 'O_CUSTKEY == C_CUSTKEY')
      keySchema = DBSchema('atts',[('L_ORDERKEY','int'),('O_ORDERDATE','int'),('O_SHIPPRIORITY','int')])
      aggrSchema = DBSchema('aggs',[('revenue','double')])
      nameSchema = DBSchema('atts', [('L_EXTENDEDPRICE','double'),('L_DISCOUNT','double')])
      query = query.groupBy( \
              groupSchema = keySchema,\
              aggSchema =aggrSchema,\
              groupExpr=(lambda e: (e.L_ORDERKEY,e.O_ORDERDATE,e.O_SHIPPRIORITY)),\
              aggExprs=[(0,lambda acc,e: e.L_EXTENDEDPRICE * (1-e.L_DISCOUNT),lambda x:x)],\
              groupHashFn=(lambda gbVal: hash((gbVal[0] + gbVal[1] + gbVal[2]) % 10))).finalize()
      if(opt):
          db.optimizer.pushdownOperators(query)
          query = db.optimizer.pickJoinOrder(query)

          results = [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1] ]
          end = time.time()
      else:

          results = [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1] ]
          end = time.time()
          #print(results)
      if(opt):
        print("query3 Optimized")
      else:
        print("query3")
      print("Execution time: " + str(end - start))


  def query4(self,db,relations,opt):
      start = time.time()
      query = db.query().fromTable(relations[0])
      #query2 = db.query().fromTable(relations[1]).where('O_ORDERDATE < 19940101')
      #query2 = query2.where('O_ORDERDATE >= 19931001')
      query3 = db.query().fromTable(relations[2]).where('L_RETURNFLAG == \'R\'')
      query3 = query3.join(\
              db.query().fromTable(relations[1]).where('O_ORDERDATE < 19940101').where('O_ORDERDATE >= 19931001'),\
              method='block-nested-loops',expr='L_ORDERKEY == O_ORDERKEY')
      query = query.join(\
              db.query().fromTable(relations[3]),\
              method='block-nested-loops',expr='C_NATIONKEY == N_NATIONKEY')
      query = query.join(\
              query3,\
              method='block-nested-loops',expr='C_CUSTKEY == O_CUSTKEY')
      keySchema = DBSchema('atts',[('C_CUSTKEY','int'),('C_NAME','char(25)'),('C_ACCTBAL','double'),\
              ('C_PHONE','char(15)'),('N_NAME','char(25)'),('C_ADDRESS','char(40)'),('C_COMMENT','char(117)')])
      aggrSchema = DBSchema('aggs',[('revenue','double')])
      nameSchema = DBSchema('atts', [('L_EXTENDEDPRICE','double'),('L_DISCOUNT','double')])
      query = query.groupBy( \
              groupSchema = keySchema,\
              aggSchema =aggrSchema,\
              groupExpr=(lambda e: (e.C_CUSTKEY,e.C_NAME,e.C_ACCTBAL,e.C_PHONE,e.N_NAME,e.C_ADDRESS,e.C_COMMENT)),\
              aggExprs=[(0,lambda acc,e: e.L_EXTENDEDPRICE * (1-e.L_DISCOUNT),lambda x:x)],\
              groupHashFn=(lambda gbVal: hash((gbVal[0]) % 10))).finalize()
      if(opt):
          db.optimizer.pushdownOperators(query)
          query = db.optimizer.pickJoinOrder(query)

          results = [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1] ]
          end = time.time()
      else:

          results = [query.schema().unpack(tup) for page in db.processQuery(query) for tup in page[1] ]
          #print(results)
          end = time.time()
      if(opt):
        print("query4 Optimized")
      else:
        print("query4")
      print("Execution time: " + str(end - start))


  def query5(self,db,relations,opt):
      start = time.time()
      #query2 = db.query().fromTable(relations[1]).where('O_ORDERDATE >= 19940101')
      #query2 = query2.where('O_ORDERDATE < 19950101')
      query3 = db.query().fromTable(relations[5]).where('R_NAME == \'ASIA\'')
      query3 = db.query().fromTable(relations[3]).join(\
              query3,\
              method='block-nested-loops',expr='N_REGIONKEY == R_REGIONKEY')
      query3 = db.query().fromTable(relations[4]).join(\
              query3,\
              method='block-nested-loops',expr='S_NATIONKEY == N_NATIONKEY')
      query3 = db.query().fromTable(relations[0]).join(\
              query3,\
              method='block-nested-loops',expr='C_NATIONKEY == S_NATIONKEY')
      query3 = db.query().fromTable(relations[2]).join(\
              query3,\
              method='block-nested-loops',expr='L_SUPPKEY == S_SUPPKEY')
      query3 = query3.join(\
              db.query().fromTable(relations[1]).where('O_ORDERDATE >= 19940101').where('O_ORDERDATE < 19950101'),\
              method='block-nested-loops',expr='L_ORDERKEY == O_ORDERKEY')
      query3 = query3.where('C_CUSTKEY == O_CUSTKEY')
      keySchema = DBSchema('atts',[('N_NAME','char(25)')])
      aggrSchema = DBSchema('aggs',[('revenue','double')])
      nameSchema = DBSchema('atts', [('L_EXTENDEDPRICE','double'),('L_DISCOUNT','double')])
      query3 = query3.groupBy( \
              groupSchema = keySchema,\
              aggSchema =aggrSchema,\
              groupExpr=(lambda e: e.N_NAME),\
              aggExprs=[(0,lambda acc,e: e.L_EXTENDEDPRICE * (1-e.L_DISCOUNT),lambda x:x)],\
              groupHashFn=(lambda gbVal: hash((gbVal[0]) % 10))).finalize()
      if(opt):
          db.optimizer.pushdownOperators(query3)
          query3 = db.optimizer.pickJoinOrder(query3)

          results = [query.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1] ]
          #print(results)
          end = time.time()
      else:

          results = [query.schema().unpack(tup) for page in db.processQuery(query3) for tup in page[1] ]
          #print(results)
          end = time.time()
      if(opt):
        print("query5 Optimized")
      else:
        print("query5")
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
          self.query1(db,['lineitem'],False)
          self.query1(db,['lineitem'],True)

      elif mode == 6:
          self.query2(db,['lineitem','part'],False)
          self.query2(db,['lineitem','part'],True)

      elif mode == 7:
          self.query3(db,['customer','orders','lineitem'],False)
          self.query3(db,['customer','orders','lineitem'],True)

      elif mode == 8:
          self.query4(db,['customer','orders','lineitem','nation'],False)
          self.query4(db,['customer','orders','lineitem','nation'],True)

      elif mode == 9:
          self.query5(db,['customer','orders','lineitem','nation','supplier','region'],False)
          self.query5(db,['customer','orders','lineitem','nation','supplier','region'],True)

      else:
        raise ValueError("Invalid workload mode (expected 1-4): "+str(mode))
    else:
      raise ValueError("No tuple ids found, has the dataset been loaded?")

  def runWorkload(self, datadir, scaleFactor, pageSize, workloadMode):
    db = Database(pageSize=pageSize)
    self.createRelations(db)
    self.loadDataset(db, datadir, scaleFactor)
    self.runOperations(db, workloadMode)
    self.runOperations(db, workloadMode + 1)
    self.runOperations(db, workloadMode + 2)
    self.runOperations(db, workloadMode + 3)
    self.runOperations(db, workloadMode + 4)
    db.close()
    shutil.rmtree(db.fileManager().dataDir, ignore_errors=True)
    del db

if __name__ == "__main__":
    import doctest
    doctest.testmod()
