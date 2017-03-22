from Catalog.Schema import DBSchema
from Query.Operator import Operator

# Operator for External Sort
class Sort(Operator):

  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)
    self.subPlan     = subPlan
    self.sortKeyFn   = kwargs.get("sortKeyFn", None)
    self.sortKeyDesc = kwargs.get("sortKeyDesc", None)
    self.pinnedPages = list()
    self.partitions = list()
    self.runs = 1

    if self.sortKeyFn is None or self.sortKeyDesc is None:
      raise ValueError("No sort key extractor provided to a sort operator")

  # Returns the output schema of this operator
  def schema(self):
    return self.subPlan.schema()

  # Returns any input schemas for the operator if present
  def inputSchemas(self):
    return [self.subPlan.schema()]

  # Returns a string describing the operator type
  def operatorType(self):
    return "Sort"

  # Returns child operators if present
  def inputs(self):
    return [self.subPlan]


  # Iterator abstraction for external sort operator.
  def __iter__(self):
    self.initializeOutput()
    self.inputIterator = iter(self.subPlan)
    self.inputFinished = False

    if not self.pipelined:
        self.outputIterator = self.processAllPages()

    return self

  def __next__(self):
    #raise NotImplementedError
    if self.pipelined:
        while not (self.inputFinished or self.isOutputPageReady()):
            try:
                pageId, page = next(self.inputIterator)
                self.processInputPage(pageId, page)

            except StopIteration:
                self.inputFinished = True
        return self.outputPage()

    else:
        return next(self.outputIterator)




  # Page processing and control methods
   
  # Page-at-a-time operator processing
  def processInputPage(self, pageId, page, count, pinnedPages, parts):
    #raise NotImplementedError
    #pinnedPages = list()
    #parts = list()
    tups = list()
    if self.storage.bufferPool.numPages() > 0 and count > 0:
        self.storage.bufferPool.pinPage(pageId)
        pinnedPages.append((pageId, page))
    
    #all pages done, final run
    else:
        #tups = list()
        for (pageId, page) in pinnedPages:
            for tup in page:
                tups.append(self.schema().unpack(tup))
            self.storage.bufferPool.unpinPage(pageId)

        pinnedPages = list()
        #parts = list()
        sortedTups = sorted(tups, key=self.sortKeyFn, reverse=True)
        #print(sortedTups)    
        #print(count)
        relationID = str(count) + "sort" 
        self.storage.createRelation(relationID, self.schema())
        parts.append(relationID)
        print(sortedTups)
        for tup in tups:
            packedTup = self.schema().pack(tup)
            self.storage.insertTuple(relationID, packedTup)

        #to end
        if count == -1:
            #last call
            partsToMerge = list(parts)
            numParts = len(partsToMerge)
            print("numParts: %d" % numParts)
            while numParts > 1:
                partIndex = 0
                nxt = partIndex + 1
                print("next: %d" % nxt)
                while nxt < partsToMerge:
                    left = self.storage.tuples(partsToMerge[partIndex])
                    right = self.storage.tuples(partsToMerge[nxt])

                    relationID = str(count) + "sort"
                    self.storage.createRelation(relationID, self.schema())
                    parts.append(relationID)
                
                    leftTuples = list()
                    rightTuples = list()
                    for tup in left:
                        leftTuples.append(self.schema().unpack(tup))
                    for tup in right:
                        rightTuples.append(self.schema().unpack(tup))

                    print("/////////////////////////////////////////////////")
                    self.merge(leftTuples, rightTuples, relationID)
                    partIndex = nxt + 1
                while partIndex > 0:
                    print("HERE")
                    #numParts = numParts - 1
                    partsToMerge.pop(0)
                    partIndex = partIndex - 1
                numParts = numParts - 1
            

            outTuples = self.storage.tuples(parts[-1])

                
            #for relationID in parts:
                #self.storage.removeRelation(relationID)

            for tup in outTuples:
                self.emitOutputTuple(tup)        
        
            
            for relationID in parts:
                self.storage.removeRelation(relationID)
        
        else:
            pinnedPages.append((pageId, page))
            self.storage.bufferPool.pinPage(pageId)

  
        
  


   
  # Set-at-a-time operator processing
  def processAllPages(self):
    #raise NotImplementedError
    pinnedPages = list()
    parts = list()
    count = 1
    for (pageId, page) in self.inputIterator:
      self.processInputPage(pageId, page, count, pinnedPages, parts)
      count = count + 1
    
    self.processInputPage(None, None, -1, pinnedPages, parts)
    
    print("DIS")
    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    return self.storage.pages(self.relationId())
  

  ''' 
  def processInputPage(self, pageId, page):
    print("Enters processInputPage")
    lastIteration = pageId is None and page is None

    bufPool = self.storage.bufferPool

    if bufPool.numFreePages() > 0 and not lastIteration:
      # Fill available memory
      bufPool.pinPage(pageId)
      self.pinnedPages.append((pageId, page))
    else:
      # Created sorted runs
      tuples = list()

      for (pageId, page) in self.pinnedPages:
        for tuple in page:
          tuples.append(self.schema().unpack(tuple))
        bufPool.unpinPage(pageId)

      self.pinnedPages = list()

      tuples = sorted(tuples, key=self.sortKeyFn, reverse=True)

      relId = "sort" + str(self.runs)
      self.storage.createRelation(relId, self.schema())
      self.partitions.append(relId)

      for tuple in tuples:
        tupleP = self.schema().pack(tuple)
        self.storage.insertTuple(relId, tupleP)

      self.runs = self.runs + 1

      if lastIteration:
        print("Enters lastIteration")
        # Perform merge join over 2 runs each
        unMergedParts = list(self.partitions)
        numPartitions = len(unMergedParts)
        print("numPartitions: %d" %numPartitions)
        print(tuples)
        while numPartitions > 1:
          print("numPartitions > 1: %d" % numPartitions) 
          idx = 0
          while idx + 1 < numPartitions:
            lRelId = unMergedParts[idx]
            rRelId = unMergedParts[idx+1]

            lTuples = self.storage.tuples(lRelId)
            rTuples = self.storage.tuples(rRelId)

            relId = "sort" + str(self.runs)
            self.storage.createRelation(relId, self.schema())
            self.partitions.append(relId)

            lTuplesLst = list()
            for lTuple in lTuples:
              lTuplesLst.append(self.schema().unpack(lTuple))

            rTuplesLst = list()
            for rTuple in rTuples:
              rTuplesLst.append(self.schema().unpack(rTuple))
            print("Enters merge")
            self.merge(lTuplesLst, rTuplesLst, relId)
            
            idx = idx + 2

            self.runs = self.runs + 1

          while idx > 0:
            unMergedParts.pop(0)
            idx = idx - 1

          numPartitions = len(unMergedParts)

        finalRelId = self.partitions[-1]

        outputTuples = self.storage.tuples(finalRelId)

        for outputTuple in outputTuples:
          self.emitOutputTuple(outputTuple)

        for relId in self.partitions:
          self.storage.removeRelation(relId)
      else:
        bufPool.pinPage(pageId)
        self.pinnedPages.append((pageId, page))
  '''
  
  def merge(self, lTuples, rTuples, relId):
    print("LEFT!!!!!!!!!!!!!!!!!!\n")
    print(lTuples)
    print("\n\n\nRIGHT!!!!!!!!!!!!!!!\n")
    print(rTuples)
    if not lTuples:
      for rTuple in rTuples:
        self.storage.insertTuple(relId, self.schema().pack(rTuple))
      return
    if not rTuples:
      for lTuple in lTuples:
        self.storage.insertTuple(relId, self.schema().pack(lTuple))
      return

    if lTuples[-1] < rTuples[-1]:
      lTuples,rTuples = rTuples,lTuples

    idx = 0

    rCard = len(rTuples)
    rTuple = rTuples[idx]
    rKey = self.sortKeyFn(rTuple)

    for lTuple in lTuples:
      lKey = self.sortKeyFn(lTuple)
      while rKey > lKey:
        idx = idx + 1
        if rTuple is not None:
          self.storage.insertTuple(relId, self.schema().pack(rTuple))
        if idx < rCard:
          rTuple = rTuples[idx]
        else:
          rTuple = None
          break
        rKey = self.sortKeyFn(rTuple)
      self.storage.insertTuple(relId, self.schema().pack(lTuple))
    if rTuple is not None:
      self.storage.insertTuple(relId, self.schema().pack(rTuple))
      idx = idx + 1
    while idx < rCard:
      self.storage.insertTuple(relId, self.schema().pack(rTuples[idx]))
      idx = idx + 1
  
  '''  
  # Set-at-a-time operator processing
  def processAllPages(self):
    for (pageId, page) in self.inputIterator:
      self.processInputPage(pageId, page)

    self.processInputPage(None, None)

    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    return self.storage.pages(self.relationId())
  '''



  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(sortKeyDesc='" + str(self.sortKeyDesc) + "')"
