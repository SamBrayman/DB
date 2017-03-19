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
    schema = self.subPlan.schema()
    for (pageId, page) in self.inputIterator:
      for inputTuple in page:
        #need tuple not as MemoryView, unpack
        unpackedTuple = self.subSchema.unpack(inputTuple)
        #print(page)
        #print(unpackedTuple)
        groupValue = (self.groupExpr(unpackedTuple),)
        partKey = self.groupHashFn(groupValue)
        if(~self.storage.hasRelation(str(partKey))):
            self.storage.createRelation(str(partKey),self.subSchema)
        self.storage.insertTuple(str(partKey),inputTuple)
        for i in range(len(self.aggExprs)):
            absLst[i] = self.aggExprs[i][1](absLst[i],unpackedTuple)
            #self.aggExprs[i] = (self.aggExprs[i][1](self.aggExprs[i][0],unpackedTuple),self.aggExprs[i][1],self.aggExprs[i][2])
            #lst.append(self.aggExprs[i][1](self.aggExprs[i][0],unpackedTuple))
        hshKeys[str(partKey)] = 0#tuple(lst)
    #for tup in self.storage.tuples():
    #    groupValue = self.groupExpr(self.subSchema.unpack(tup))
    #    for i in range(len(self.aggExprs)):
    #        if(groupValue == self.aggExprs[i][0]):
    #            hshKeys.append(self.groupHashFn(groupValue))
    #finalAgs = []
    #for i in range(len(self.aggExprs)):
    #    finalAgs.append(self.aggExprs[i][0])
    #finalAgs = tuple(finalAgs)
    #extremes = []
    #for i in range(len(self.aggExprs)):
    #    extremes.append(0)
    #for key in hshKeys:
    #    for i in range(len(self.aggExprs)):
    #        extremes[i] = self.aggExprs[i][1](extremes[i],hshKeys[key])
    #for key in hshKeys:
        #print(hshKeys[key][0])
        #print(finalAgs)
        #want = False
        #for i in range(len(self.aggExprs)):
        #    if(self.aggExprs[i][1](self.aggExprs[i][0]) == hshKeys):
        #        want = true
        #if want:#self.aggExprs[][]hshKeys[key] == self.aggExprs:
    #for (pageId,page) in self.storage.pages(key):
    #absLst = []
    #for i in range(len(self.aggExprs)):
    #    absLst.append(self.aggExprs[i][0])
    #print(absLst)
    #print(len(hshKeys))
    for key in hshKeys:
        #for (pageId,page) in self.storage.pages(key):
    #for (pageId, page) in self.inputIterator:
            #print(page)
            #for tup in page:
            for tup in self.storage.tuples(key):
                #print(tup)
                #want = False
                upackedTuple = self.subSchema.unpack(tup)
                print(unpackedTuple)
                for i in range(len(self.aggExprs)):
                    #print(want)
                    if(self.aggExprs[i][1](self.aggExprs[i][0],unpackedTuple) == absLst[i]):
                        #print(str(absLst[i]) + " " + str(unpackedTuple.age))
                        #print(want)
                        groupByExprEnv = self.loadSchema(schema, tup)
                        #if eval(self.groupByExpr, globals(), groupByExprEnv):
                        #outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                        self.emitOutputTuple(tup)
                        if self.outputPages:
                            self.outputPages = [self.outputPages[-1]]
    return self.storage.pages(self.relationId())







  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(groupSchema=" + self.groupSchema.toString() \
                             + ", aggSchema=" + self.aggSchema.toString() + ")"
