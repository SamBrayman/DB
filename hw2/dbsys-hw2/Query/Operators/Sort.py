from Catalog.Schema import DBSchema
from Query.Operator import Operator

# Operator for External Sort
class Sort(Operator):

  def __init__(self, subPlan, **kwargs):
    super().__init__(**kwargs)
    self.subPlan     = subPlan
    self.sortKeyFn   = kwargs.get("sortKeyFn", None)
    self.sortKeyDesc = kwargs.get("sortKeyDesc", None)

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
    tups = list()
    #all runs but last, when count == -1
    if self.storage.bufferPool.numFreePages() > 0 and count > 0:
        self.storage.bufferPool.pinPage(pageId)
        pinnedPages.append((pageId, page))
    
    #all pages done, final run
    else:
        for (pageId, page) in pinnedPages:
            for tup in page:
                tups.append(self.schema().unpack(tup))
            self.storage.bufferPool.unpinPage(pageId)

        pinnedPages = list()
        sortedTups = sorted(tups, key=self.sortKeyFn, reverse=True)
        relationID = "countIs" + str(count) 
        self.storage.createRelation(relationID, self.schema())
        parts.append(relationID)
        #print(sortedTups)
        for tup in sortedTups:
            packedTup = self.schema().pack(tup)
            self.storage.insertTuple(relationID, packedTup)

        #no test for this....
        #merge never called???
        #to end
        if count == -1:
            #last call
            partsToMerge = list(parts)
            numParts = len(partsToMerge)
            #print(sortedTups)
            while numParts > 1:
                partIndex = 0
                nxt = partIndex + 1
                #print("next: %d" % nxt)
                while nxt < partsToMerge:
                    left = self.storage.tuples(partsToMerge[partIndex])
                    right = self.storage.tuples(partsToMerge[nxt])

                    relationID = "countIs" + str(count)
                    self.storage.createRelation(relationID, self.schema())
                    parts.append(relationID)
                
                    leftTuples = list()
                    rightTuples = list()
                    for tup in left:
                        leftTuples.append(self.schema().unpack(tup))
                    for tup in right:
                        rightTuples.append(self.schema().unpack(tup))

                    
                    ######### START MERGE #############
                    #execute actual merge
                    test = True

                    #error check for if no left or right tups
                    if (len(right) == 0) and (len(left) == 0):
                        test = False

                    #error check for if no left tuples
                    if len(left) == 0:
                        for tup in right:
                            self.storage.insertTuple(relationID, self.schema().pack(tup))
                        test = False
                    #error check for if no right tuples
                    if len(right) == 0:
                        for tup in left:
                            self.storage.insertTuple(relationID, self.schema().pack(tup))
                        test = False
                    
                    #arbitrarily choosing right
                    #check last element
                    if test and (right[-1] > left[-1]):
                        temp = left
                        left = right
                        right = temp
                    
                    #start index
                    index = 0

                    rightLen = len(right)
                    leftLen = len(right)
                    
                    #rightKey = self.sortKeyFn(
                    if test:
                        rightTup = right[index]
                        rightValue = self.sortKeyFn(rightTup)
                    
                        for tup in left:
                            leftValue = self.sortKeyFn(tup)
                            while rightValue > leftValue:
                                index += 1
                                if rightTup is not None:
                                    self.storage.insertTuple(relationID, self.schema().pack(rightTup))
                                if index < rightLen:

                                    rightTup = right[index]
                                else:
                                    rightTup = None
                                    break
                                rightValue = self.sortKeyFn(rightTup)
                            #now insert left tuple
                            self.storage.insertTuple(relationID, self.schema().pack(tup))
                        if rightTup is not None:
                            self.storage.insertTuple(relationID, self.schema().pack(rightTup))
                            index += 1
                        #finish out tups
                        while index < rightLen and test:
                            tTup = right[index]
                            self.storage.insertTuple(relationID, self.schema().pack(tTup))
                            index += 1

                    ########### END MERGE #################
                    partIndex = nxt + 1
                while partIndex > 0:
                    #print("HERE")
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
      count += 1
    
    self.processInputPage(None, None, -1, pinnedPages, parts)
    
    if self.outputPages:
      self.outputPages = [self.outputPages[-1]]

    return self.storage.pages(self.relationId())
  
  
  # Plan and statistics information

  # Returns a single line description of the operator.
  def explain(self):
    return super().explain() + "(sortKeyDesc='" + str(self.sortKeyDesc) + "')"
