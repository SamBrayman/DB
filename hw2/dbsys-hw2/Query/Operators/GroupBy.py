from Catalog.Schema import DBSchema
from Query.Operator import Operator

class GroupBy(Operator):
  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined group-by-aggregate operator not supported")

    self.subPlan     = subPlan
    self.subSchema   = subPlan.schema()
    self.groupSchema = kwargs.get("groupSchema", None)
    self.aggSchema   = kwargs.get("aggSchema", None)
    self.groupExpr   = kwargs.get("groupExpr", None)
    self.aggExprs    = kwargs.get("aggExprs", None)
    self.groupHashFn = kwargs.get("groupHashFn", None)
    self.groupByExpr = None

    self.validateGroupBy()
    self.initializeSchema()

  # Perform some basic checking on the group-by operator's parameters.
  def validateGroupBy(self):
    requireAllValid = [self.subPlan, \
                       self.groupSchema, self.aggSchema, \
                       self.groupExpr, self.aggExprs, self.groupHashFn ]

    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete group-by specification, missing a required parameter")

    if not self.aggExprs:
      raise ValueError("Group-by needs at least one aggregate expression")

    if len(self.aggExprs) != len(self.aggSchema.fields):
      raise ValueError("Invalid aggregate fields: schema mismatch")

  # Initializes the group-by's schema as a concatenation of the group-by
  # fields and all aggregate fields.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.groupSchema.schema() + self.aggSchema.schema()
    self.outputSchema = DBSchema(schema, fields)

  # Returns the output schema of this operator
  def schema(self):
    return self.outputSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "GroupBy"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]

  # Iterator abstraction for selection operator.
  def __iter__(self):
    #raise NotImplementedError
    self.initializeOutput()
    self.inputIterator = iter(self.subPlan)
    self.outputIterator = self.processAllPages()
    return self

  def __next__(self):
    #raise NotImplementedError
    return next(self.outputIterator)

  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page):
    raise ValueError("Page-at-a-time processing not supported for joins")

  # Set-at-a-time operator processing
  def processAllPages(self):
    #raise NotImplementedError
    hshKeys = {}
    absLst = []
    for i in range(len(self.aggExprs)):
        absLst.append(self.aggExprs[i][0])
    schema = self.schema()
    bufPool = self.storage.bufferPool
    for (pageId, page) in iter(self.subPlan):
      for inputTuple in page:
        unpackedTuple = self.subSchema.unpack(inputTuple)
        groupValue = (self.groupExpr(unpackedTuple),)
        partKey = self.groupHashFn(groupValue)
        self.storage.createRelation(str(partKey),self.outputSchema)
        lst = []
        for i in range(len(self.aggExprs)):
            absLst[i] = self.aggExprs[i][1](absLst[i],unpackedTuple)
            lst.append(self.aggExprs[i][1](self.aggExprs[i][0],unpackedTuple))
        self.storage.insertTuple(str(partKey),inputTuple)
        hshKeys[str(partKey)] = 0#tuple(lst)
    for key in hshKeys:
        relLst = []
        relLst.append(int(key))
        for i in range(len(self.aggExprs)):
            relLst.append(self.aggExprs[i][0])
        for tup in self.storage.tuples(key):
            unpackedTuple = self.subSchema.unpack(tup)
            for i in range(len(self.aggExprs)):
                relLst[i+1] = self.aggExprs[i][1](relLst[i+1],unpackedTuple)
        self.storage.removeRelation(key)
        self.storage.createRelation(key,self.schema())
        newTup = self.schema().pack(tuple(relLst))
        #print(self.schema().unpack(newTup))
        self.storage.insertTuple(key,newTup)
    for key in hshKeys:
        for (pageId,page) in self.storage.pages(key):
            for tup in page:
                want = False
                unpackedTuple = self.schema().unpack(tup)
                for i in range(len(self.aggExprs)):
                    if(unpackedTuple[i+1]== absLst[i]):
                        #print(str(absLst[i]) + " " + str(unpackedTuple.age))
                        #print(want)
                        #unpackedTuple = self.schema().unpack(tup)
                        #intup = (unpackedTuple)
                        #print(unpackedTuple)
                        groupByExprEnv = self.loadSchema(self.schema(), tup)
                        #print(groupByExprEnv)
                        #print(globals())
                        #print(self.groupByExpr)
                        #if eval(self.aggExprs, globals(), groupByExprEnv):
                        outputTuple = self.outputSchema.instantiate(*[groupByExprEnv[f] for f in self.outputSchema.fields])
                        #print(outputTuple)
                        self.emitOutputTuple(self.outputSchema.pack(outputTuple))
            if self.outputPages:
                self.outputPages = [self.outputPages[-1]]
    for key in hshKeys:
        self.storage.removeRelation(key)
    return self.storage.pages(self.relationId())







  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"
