from itertools import chain

from Catalog.Schema import DBSchema
from Query.Operator import Operator
from Storage.BufferPool import BufferPool

class Join(Operator):
  def __init__(self, lhsPlan, rhsPlan, **kwargs):
    super().__init__(**kwargs)

    if self.pipelined:
      raise ValueError("Pipelined join operator not supported")

    self.lhsPlan    = lhsPlan
    self.rhsPlan    = rhsPlan
    self.joinExpr   = kwargs.get("expr", None)
    self.joinMethod = kwargs.get("method", None)
    self.lhsSchema  = kwargs.get("lhsSchema", None if lhsPlan is None else lhsPlan.schema())
    self.rhsSchema  = kwargs.get("rhsSchema", None if rhsPlan is None else rhsPlan.schema())

    self.lhsKeySchema   = kwargs.get("lhsKeySchema", None)
    self.rhsKeySchema   = kwargs.get("rhsKeySchema", None)
    self.lhsHashFn      = kwargs.get("lhsHashFn", None)
    self.rhsHashFn      = kwargs.get("rhsHashFn", None)

    self.validateJoin()
    self.initializeSchema()
    self.initializeMethod(**kwargs)

  # Checks the join parameters.
  def validateJoin(self):
    # Valid join methods: "nested-loops", "block-nested-loops", "indexed", "hash"
    if self.joinMethod not in ["nested-loops", "block-nested-loops", "indexed", "hash"]:
      raise ValueError("Invalid join method in join operator")

    # Check all fields are valid.
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      methodParams = [self.joinExpr]

    elif self.joinMethod == "indexed":
      methodParams = [self.lhsKeySchema]

    elif self.joinMethod == "hash":
      methodParams = [self.lhsHashFn, self.lhsKeySchema, \
                      self.rhsHashFn, self.rhsKeySchema]

    requireAllValid = [self.lhsPlan, self.rhsPlan, \
                       self.joinMethod, \
                       self.lhsSchema, self.rhsSchema ] \
                       + methodParams
    #print(requireAllValid)
    if any(map(lambda x: x is None, requireAllValid)):
      raise ValueError("Incomplete join specification, missing join operator parameter")

    # For now, we assume that the LHS and RHS schema have
    # disjoint attribute names, enforcing this here.
    for lhsAttr in self.lhsSchema.fields:
      if lhsAttr in self.rhsSchema.fields:
        raise ValueError("Invalid join inputs, overlapping schema detected")


  # Initializes the output schema for this join.
  # This is a concatenation of all fields in the lhs and rhs schema.
  def initializeSchema(self):
    schema = self.operatorType() + str(self.id())
    fields = self.lhsSchema.schema() + self.rhsSchema.schema()
    self.joinSchema = DBSchema(schema, fields)

  # Initializes any additional operator parameters based on the join method.
  def initializeMethod(self, **kwargs):
    if self.joinMethod == "indexed":
      self.indexId = kwargs.get("indexId", None)
      if self.indexId is None or self.lhsKeySchema is None:
        raise ValueError("Invalid index for use in join operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.joinSchema

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.lhsSchema, self.rhsSchema]

  # Returns a string describing the operator type
  def operatorType(self):
    readableJoinTypes = { 'nested-loops'       : 'NL'
                        , 'block-nested-loops' : 'BNL'
                        , 'indexed'            : 'Index'
                        , 'hash'               : 'Hash' }
    return readableJoinTypes[self.joinMethod] + "Join"

  # Returns child operators if present
  def inputs(self):
    return [self.lhsPlan, self.rhsPlan]

  # Iterator abstraction for join operator.
  def __iter__(self):
    #raise NotImplementedError
    self.initializeOutput()
    self.inputIterator = chain(self.lhsPlan,self.rhsPlan)
    self.inputFinished = False
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
    if self.joinMethod == "nested-loops":
      return self.nestedLoops()

    elif self.joinMethod == "block-nested-loops":
      return self.blockNestedLoops()

    elif self.joinMethod == "indexed":
      return self.indexedNestedLoops()

    elif self.joinMethod == "hash":
      return self.hashJoin()

    else:
      raise ValueError("Invalid join method in join operator")


  ##################################
  #
  # Nested loops implementation
  #
  def nestedLoops(self):
    for (lPageId, lhsPage) in iter(self.lhsPlan):
      for lTuple in lhsPage:
        # Load the lhs once per inner loop.
        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)

        for (rPageId, rhsPage) in iter(self.rhsPlan):
          for rTuple in rhsPage:
            # Load the RHS tuple fields.
            joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))

            # Evaluate the join predicate, and output if we have a match.
            if eval(self.joinExpr, globals(), joinExprEnv):
              outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
              self.emitOutputTuple(self.joinSchema.pack(outputTuple))

        # No need to track anything but the last output page when in batch mode.
        if self.outputPages:
          self.outputPages = [self.outputPages[-1]]

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())


  ##################################
  #
  # Block nested loops implementation
  #
  # This attempts to use all the free pages in the buffer pool
  # for its block of the outer relation.

  # Accesses a block of pages from an iterator.
  # This method pins pages in the buffer pool during its access.
  # We track the page ids in the block to unpin them after processing the block.
  def accessPageBlock(self, bufPool, pageIterator):
    #raise NotImplementedError
    for (pageId,page) in pageIterator:
        #if(bufPool.numFreePages() != 2):
        if ~bufPool.hasPage(pageId):
            bufPool.getPage(pageId)
            bufPool.pinPage(pageId)
        else:
            bufPool.pinPage(pageId)
        #else:
        #    break
  def accessPageBlock2(self, bufPool, pageIterator):
    #raise NotImplementedError
    for (pageId,page) in pageIterator:
        #if(bufPool.numFreePages() != 2):
        bufPool.unpinPage(pageId)
        #else:
        #    break

  def blockNestedLoops(self):
    #raise NotImplementedError
    bufPool = self.storage.bufferPool
    blocks = [[]]
    ind = 0
    for (pageId,page) in iter(self.lhsPlan):
        if(len(blocks[ind]) < self.storage.bufferPool.numPages()-2):
            blocks[ind].append((pageId,page))
        else:
            ind += 1
            blocks.append([])
            blocks[ind].append((pageId,page))
    for block in blocks:
        self.accessPageBlock(bufPool,block)
        for tup in block:
            for lTuple in tup[1]:
                joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)
                for (rpageId,rhsPage) in iter(self.rhsPlan):
                    for rTuple in rhsPage:
                        # Load the RHS tuple fields.
                        joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))
                        #Evaluate the join predicate, and output if we have a match.
                        #print(globals())
                        if eval(self.joinExpr, globals(), joinExprEnv):
                            outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                            self.emitOutputTuple(self.joinSchema.pack(outputTuple))
                # No need to track anything but the last output page when in batch mode.
                if self.outputPages:
                    self.outputPages = [self.outputPages[-1]]
        self.accessPageBlock2(bufPool,block)

    # Return an iterator to the output relation
    return self.storage.pages(self.relationId())

  ##################################
  #
  # Indexed nested loops implementation
  #
  # TODO: test
  def indexedNestedLoops(self):
    raise NotImplementedError

  ##################################
  #
  # Hash join implementation.
  #
  def hashJoin(self):
    #raise NotImplementedError
    count = 1
    bufPool = self.storage.bufferPool
    blocksL = [[]]
    blocksR = [[]]
    keysL = {}
    keysR = {}
    ind = 0
    #print(type(self.lhsHashFn))
    for (pageId,page) in iter(self.lhsPlan):
        for lTuple in page:
            partKey = eval(self.lhsHashFn,globals(),self.loadSchema(self.lhsSchema,lTuple))
            self.storage.createRelation(str(partKey) + str(0),self.lhsSchema)
            self.storage.insertTuple(str(partKey) + str(0),lTuple)
            keysL[partKey] = 0
    for (pageId,page) in iter(self.rhsPlan):
        for rTuple in page:
            partKey = eval(self.rhsHashFn,globals(),self.loadSchema(self.rhsSchema,rTuple))
            self.storage.createRelation(str(partKey) + str(1),self.rhsSchema)
            self.storage.insertTuple(str(partKey) + str(1),rTuple)
            keysR[partKey] = 1
    for key1 in keysL.keys():
        if(key1 in keysR.keys()):
            ind = 0
            blocksL = [[]]
            blocksR = [[]]
            for (pageId,page) in self.storage.pages(str(key1) + str(0)):
                #print(len(blocksL))
                #print(ind)
                #print(key1)
                if(len(blocksL[ind]) < self.storage.bufferPool.numPages()-2):
                    blocksL[ind].append((pageId,page))
                else:
                    ind += 1
                    blocksL.append([])
                    blocksL[ind].append((pageId,page))
            ind = 0
            for (pageId,page) in self.storage.pages(str(key1) + str(1)):
                if(len(blocksR[ind]) < self.storage.bufferPool.numPages()-2):
                    blocksR[ind].append((pageId,page))
                else:
                    ind += 1
                    blocksR.append([])
                    blocksR[ind].append((pageId,page))
            ind = 0
            for block in blocksL:
                self.accessPageBlock(bufPool,block)
                for tup in block:
                    for lTuple in tup[1]:
                        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)
                        #for block1 in blocksR:
                        block1 = blocksR[ind]
                        self.accessPageBlock(bufPool,block1)
                        for tup1 in block1:
                            for rTuple in tup1[1]:
                                joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))
                                    #schema.project(e1, projectedSchema)
                                unpack1 = self.lhsSchema.unpack(lTuple)
                                unpack2 = self.rhsSchema.unpack(rTuple)
                                    #if self.lhsKeySchema.project(unpack1,self.lhsKeySchema) == self.rhsKeySchema.project(unpack2,self.rhsKeySchema):#eval(self.lhsKeySchema,globals(),self.rhsKeySchema):#eval(self.joinExpr, globals(), joinExprEnv):
                                if unpack1 == unpack2:
                                    #print(unpack1)
                                    #print(unpack2)
                                    #print(count)
                                    count += 1
                                    outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
                                    self.emitOutputTuple(self.joinSchema.pack(outputTuple))
                        self.accessPageBlock2(bufPool,block1)
                        if self.outputPages:
                            self.outputPages = [self.outputPages[-1]]
                self.accessPageBlock2(bufPool,block)
                ind +=1
    for key1 in keysL.keys():
        self.storage.removeRelation(key1)
    for key1 in keysR.keys():
        self.storage.removeRelation(key1)
    return self.storage.pages(self.relationId())



    #for (pageId,page) in iter(self.lhsPlan):
    #    if(len(blocksL[ind]) < self.storage.bufferPool.numPages()-2):
    #        blocksL[ind].append((pageId,page))
#        else:
#          ind += 1
#            blocksL.append([])
#            blocksL[ind].append((pageId,page))
#    for block in blocksL:
#        self.accessPageBlock(bufPool,block)
#        for tup in block:
#            for lTuple in tup[1]:
#                partKey = eval(self.lhsHashFn,globals(),self.loadSchema(self.lhsSchema,lTuple))
#                self.storage.createRelation(str(partKey),self.lhsSchema)
#                self.storage.insertTuple(str(partKey),lTuple)
#                keysL[partKey] = 0
#    ind = 0
#    for (pageId,page) in iter(self.rhsPlan):
#        if(len(blocksR[ind]) < self.storage.bufferPool.numPages()-2):
#            blocksR[ind].append((pageId,page))
#        else:
#            ind += 1
#            blocksR.append([])
#            blocksR[ind].append((pageId,page))
#    for block in blocksR:
#        self.accessPageBlock(bufPool,block)
#        for tup in block:
#            for rTuple in tup[1]:
#                partKey = eval(self.rhsHashFn,globals(),self.loadSchema(self.rhsSchema,rTuple))
#                self.storage.createRelation(str(partKey),self.rhsSchema)
#                self.storage.insertTuple(str(partKey),rTuple)
#                keysR[partKey] = 1
#    for key1 in keysL:
#        for key2 in keysR:
#            if(key1 == key2):
#                for (pageId,page) in self.storage.pages(key1):
#                    for tup in page:
#                        joinExprEnv = self.loadSchema(self.lhsSchema, lTuple)
#                    for (rpageId,rhsPage) in iter(self.rhsPlan):
#                    for rTuple in rhsPage:
#                        # Load the RHS tuple fields.
#                        joinExprEnv.update(self.loadSchema(self.rhsSchema, rTuple))
#                        #Evaluate the join predicate, and output if we have a match.
#                        #print(globals())
#                        if eval(self.joinExpr, globals(), joinExprEnv):
#                            outputTuple = self.joinSchema.instantiate(*[joinExprEnv[f] for f in self.joinSchema.fields])
#                            self.emitOutputTuple(self.joinSchema.pack(outputTuple))
#                # No need to track anything but the last output page when in batch mode.
#                if self.outputPages:
#                    self.outputPages = [self.outputPages[-1]]
#        self.accessPageBlock2(bufPool,block)
  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    if self.joinMethod == "nested-loops" or self.joinMethod == "block-nested-loops":
      exprs = "(expr='" + str(self.joinExpr) + "')"

    elif self.joinMethod == "indexed":
      exprs =  "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "indexKeySchema=" + self.lhsKeySchema.toString() ]
        ))) + ")"

    elif self.joinMethod == "hash":
      exprs = "(" + ','.join(filter(lambda x: x is not None, (
          [ "expr='" + str(self.joinExpr) + "'" if self.joinExpr else None ]
        + [ "lhsKeySchema=" + self.lhsKeySchema.toString() ,
            "rhsKeySchema=" + self.rhsKeySchema.toString() ,
            "lhsHashFn='" + self.lhsHashFn + "'" ,
            "rhsHashFn='" + self.rhsHashFn + "'" ]
        ))) + ")"

    return super().explain() + exprs
