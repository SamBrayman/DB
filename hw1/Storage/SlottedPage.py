import functools, math, struct
from struct import Struct
from io import BytesIO
import sys
from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema import DBSchema
from Storage.Page import PageHeader, Page

###########################################################
# DESIGN QUESTION 1: should this inherit from PageHeader?
# If so, what methods can we reuse from the parent?
#
class SlottedPageHeader(PageHeader,BytesIO):
  """
  A slotted page header implementation. This should store a slot bitmap
  implemented as a memoryview on the byte buffer backing the page
  associated with this header. Additionally this header object stores
  the number of slots in the array, as well as the index of the next
  available slot.

  The binary representation of this header object is: (numSlots, nextSlot, slotBuffer)

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = SlottedPageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = SlottedPageHeader.unpack(buffer.getbuffer())

  ## Dirty bit tests
  >>> ph.isDirty()
  False
  >>> ph.setDirty(True)
  >>> ph.isDirty()
  True
  >>> ph.setDirty(False)
  >>> ph.isDirty()
  False

  ## Tuple count tests
  >>> ph.hasFreeTuple()
  True

  # First tuple allocated should be at the first slot.
  # Notice this is a slot index, not an offset as with contiguous pages.
  >>> ph.nextFreeTuple() == 0
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0, tuplesToTest)]
  [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]

  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize + 2))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [11, 12, ...]

  >>> ph.hasFreeTuple()
  False

  # No value is returned when trying to exceed the page capacity.
  >>> ph.nextFreeTuple() == None
  True

  >>> ph.freeSpace() < ph.tupleSize
  True
  """
  # Binary representation of a page header:
  # page status flag as a char, and tuple size, page capacity
  # and free space offset as unsigned shorts.
  #freeoffset = 0
  #Storage = []#put in indices for all possible indices
  #slots = 0
  #slotBuffer = ()
  #slotsdata = []
  binrepr   = struct.Struct("cHHHHHH")
  reprSize      = binrepr.size
  def __init__(self, **kwargs):
    buffer     = kwargs.get("buffer", None)
    self.flags = kwargs.get("flags", b'\x00')
    if buffer:
      #raise NotImplementedError
      self.flags           = kwargs.get("flags", b'\x00')
      self.tupleSize       = kwargs.get("tupleSize", 0)
      self.pageCapacity    = kwargs.get("pageCapacity", len(buffer))

      self.slots = kwargs.get("slots",0)
      self.freeoffset = kwargs.get("offset",0)
      self.slotBuffer = kwargs.get("slotBuffer",None)
      self.numSlotsPos =kwargs.get("numSlotsPos", int((len(buffer) - struct.calcsize("cHHHHHH")) / (self.tupleSize + struct.calcsize("H"))))
      self.Storage = []
      for i in range(self.numSlotsPos):
            self.Storage.append(0)
      count = 0
      if(self.slotBuffer):
        for elem in self.slotBuffer:
            self.Storage[count] = elem
            count += 1
      self.freeSpaceOffset = kwargs.get("freeSpaceOffset", 0)    #None
      buffer[0:self.headerSize()] = self.pack()
    else:
      raise ValueError("No backing buffer supplied for SlottedPageHeader")

  def __eq__(self, other):
    #raise NotImplementedError
    same = False
    if(self.slots != other.slots):
        return False
    else:
        for i in range(self.slots):
            same = (self.Storage[i] == other.Storage[i])
        return self._eq_(self, other) and self.slots == other.slots and self.freeoffset == other.freeoffset and same


  def __hash__(self):
    #raise NotImplementedError
    return hash((PageHeader._hash_(self), self.slots, self.freeoffset,self.Storage))

  def headerSize(self):
    temp = ""
    for i in range(len(self.Storage)):
        temp += "H"
    return struct.calcsize("cHHHHHH" + temp)

  # Flag operations.
  def flag(self, mask):
    return (ord(self.flags) & mask) > 0

  def setFlag(self, mask, set):
    if set:
      self.flags = bytes([ord(self.flags) | mask])
    else:
      self.flags = bytes([ord(self.flags) & ~mask])

  # Dirty bit accessors
  def isDirty(self):
    return self.flag(PageHeader.dirtyMask)

  def setDirty(self, dirty):
    self.setFlag(PageHeader.dirtyMask, dirty)

  def numTuples(self):
    #raise NotImplementedError
    return self.slots

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    #raise NotImplementedError
    return self.pageCapacity - self.headerSize() - ((self.numTuples()) * self.tupleSize + 2)
    #return self.pageCapacity - self.usedSpace()
  # Returns the space used in the page associated with this header.
  def usedSpace(self):
    #raise NotImplementedError
    return self.numTuples() * self.tupleSize


  # Slot operations.
  def offsetOfSlot(self, slot):
    #raise NotImplementedError
    #iterate through buffer find slot data and return index
    return ((slot * self.tupleSize) + self.headerSize())


  def hasSlot(self, slotIndex):
    #raise NotImplementedError
    if(slotIndex < len(self.Storage)):
        return True
        #return self.Storage[slotIndex] == 1
    else:
        return False

  def getSlot(self, slotIndex):
    #raise NotImplementedError
    #iterate through buffer to find data
    if(slotIndex <len(self.Storage)):
        #start = (self.headerSize + ((slotIndex)*self.tupleSize))
        #end = start + self.tupleSize
        #return self.getbuffer()[start:end]
        return self.Storage[slotIndex]
    else:
        return None

  def setSlot(self, slotIndex, slot):
    #raise NotImplementedError
    #what is this slot object
    if(slotIndex < len(self.Storage)):
        #start = self.offsetOfSlot(slotIndex)
        #end = start + self.tupleSize
        #self.buffer[start:end] = slot
        prev = self.Storage[slotIndex]
        self.Storage[slotIndex] = slot
        if(slot-prev == 1):
            self.slots += 1
        elif(slot - prev == -1):
            self.slots -= 1
        #self.slots+=1
    else:
        raise ValueError("The index was out of bounds.")

  def resetSlot(self, slotIndex):
    #raise NotImplementedError
    if(slotIndex < len(self.Storage)):
        self.Storage[slotIndex] = 0
    else:
        raise ValueError("The index was out of bounds.")

  def freeSlots(self):
    #raise NotImplementedError
    return int(self.freeSpace()/self.tupleSize)

  def usedSlots(self):
    #raise NotImplementedError
    return int(self.usedSpace()/self.tupleSize)

  # Tuple allocation operations.
  #self.getbuffer()[starind:endind]
  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    #raise NotImplementedError
    return self.freeSpace() > self.tupleSize + 2
    #return self.PageHeader.hasFreeTuple()

  # Returns the tupleIndex of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    #raise NotImplementedError
    #iterate through storage find free space indicate thta space as allocated
    #if(self.hasFreeTuple() == False):
    #    return None
    #else:
        index = 0
        while( index < len(self.Storage) and self.Storage[index] != 0):
            index += 1
        if(index < len(self.Storage)):
            #self.Storage[index] = 1
            #self.slots += 1
            self.setSlot(index,1)
            self.freeoffset = index
            return index


  def nextTupleRange(self):
    #raise NotImplementedError
    if(self.hasFreeTuple()):
        index = 0
        while(i <self.slots and index ==0):
            if(self.Storage[i] == 0):
                index = i
            i = i + 1
        start = (self.headerSize + ((index)*self.tupleSize))
        end = start + self.tupleSize
        return (index, start,end)
    else:
        return None

  # Create a binary representation of a slotted page header.
  # The binary representation should include the slot contents.
  def pack(self):
    #raise NotImplementedError
    #pack each index in the storage array
    temp = b""
    for i in range(len(self.Storage)):
        temp = temp + struct.pack('H',self.Storage[i])
    binrepr = struct.Struct("cHHH")
    supPack = binrepr.pack(self.flags, self.tupleSize, self.freeSpaceOffset, self.pageCapacity)
    #do we want to pack the supPack?
    return supPack + struct.pack('H',self.slots) + struct.pack('H',self.numSlotsPos) + struct.pack('H',self.freeoffset) + temp

  # Create a slotted page header instance from a binary representation held in the given buffer.
  @classmethod
  def unpack(cls, buffer):
    #raise NotImplementedError
    binrepr = struct.Struct("cHHHHHH")
    values = binrepr.unpack_from(BytesIO(buffer).getbuffer())
    temp = ""
    for i in range(values[5]):
        temp += "H"
    binrepr = struct.Struct("cHHHHHH" + temp)
    values2 = None
    slotBuffer = []
    if(values[5] != 0):
        values2 = binrepr.unpack_from(BytesIO(buffer).getbuffer())#,offset=(binrepr.size))
        for elem in values2[7:]:
            slotBuffer.append(int(elem))
    else:
        slotBuffer = []
    if (len(values) == (7)):
    	return cls(buffer=buffer,offset=values[6],numSlotsPos = values[5],slotBuffer =slotBuffer,slots = values[4],tupleSize = values[1],flags = values[0])



######################################################
# DESIGN QUESTION 2: should this inherit from Page?
# If so, what methods can we reuse from the parent?
#
class SlottedPage(Page):
  """
  A slotted page implementation.

  Slotted pages use the SlottedPageHeader class for its headers, which
  maintains a set of slots to indicate valid tuples in the page.

  A slotted page interprets the tupleIndex field in a TupleId object as
  a slot index.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = SlottedPage(pageId=pId, buffer=bytes(4096), schema=schema)

  # Validate header initialization
  >>> p.header.numTuples() == 0 and p.header.usedSpace() == 0
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

  >>> tId.tupleIndex
  0

  # Retrieve the previous tuple
  >>> e2 = schema.unpack(p.getTuple(tId))
  >>> e2
  employee(id=1, age=25)

  # Update the tuple.
  >>> e1 = schema.instantiate(1,28)
  >>> p.putTuple(tId, schema.pack(e1))

  # Retrieve the update
  >>> e3 = schema.unpack(p.getTuple(tId))
  >>> e3
  employee(id=1, age=28)

  # Compare tuples
  >>> e1 == e3
  True

  >>> e2 == e3
  False

  # Check number of tuples in page
  >>> p.header.numTuples() == 1
  True

  # Ad some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples() == 11
  True

  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0)
  >>> sizeBeforeClear = p.header.usedSpace()
  >>> p.clearTuple(tId)

  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> p.header.usedSpace() == sizeBeforeClear
  True

  # Check that clearTuple only affects a tuple's contents, not its presence.
  >>> [schema.unpack(tup).age for tup in p]
  [0, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test removal of first tuple
  >>> sizeBeforeRemove = p.header.usedSpace()
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Check that the page's slots have tracked the deletion.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  """

  headerClass = SlottedPageHeader

  # Slotted page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a SlottedPageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a SlottedPageHeader.



  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)
    BytesIO.__init__(self, buffer)
    self.pageId = kwargs.get("pageId", 0)
    header      = kwargs.get("header", None)
    schema      = kwargs.get("schema", None)
    if buffer:
      if self.pageId and header:
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")

      #raise NotImplementedError

    else:
      raise ValueError("No backing buffer provided to page constructor.")


  # Header constructor override for directory pages.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return SlottedPageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a slotted page.")

  # Tuple iterator.
  def __iter__(self):
    #raise NotImplementedError
    self.iterTupleIdx = 0
    return self

  def __next__(self):
    #raise NotImplementedError
    t = self.getTuple(TupleId(self.pageId, self.iterTupleIdx))
    if t and self.iterTupleIdx < len(self.header.Storage):
      self.iterTupleIdx += 1
      return t
    else:
      raise StopIteration

  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
    #raise NotImplementedError
    if(self.header.getSlot(tupleId.tupleIndex) == 1):
        start = self.header.offsetOfSlot(tupleId.tupleIndex)
        end = start + self.header.tupleSize
        return self.getbuffer()[start:end]
    #else:
    #    return None

  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    #raise NotImplementedError
    if(tupleId.tupleIndex < len(self.header.Storage)):
        self.header.setSlot(tupleId.tupleIndex,1)
        start = self.header.offsetOfSlot(tupleId.tupleIndex)
        end = start + self.header.tupleSize
        self.getbuffer()[start:end] = tupleData
        self.header.setDirty(True)
    else:
        raise ValueError("Tuple cannot be inserted because page is full.")


  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    #raise NotImplementedError
    if(self.header.hasFreeTuple()):
        index = self.header.nextFreeTuple()
        #print(index)
        #print(self.header.Storage[index])
        if(index is not None):
          self.putTuple(TupleId(self.pageId,index),tupleData)
          self.header.setDirty(True)
          return TupleId(self.pageId,index)
        #else:
          #print("Index given is invalid.")
    #else:
        #print("Tuple cannot be inserted because there is no free tuple.")

  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    #raise NotImplementedError
    start = self.header.offsetOfSlot(tupleId.tupleIndex)
    end = start + self.header.tupleSize
    self.getbuffer()[start:end] = bytearray(self.header.tupleSize)
    self.header.setDirty(True)

  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    #raise NotImplementedError
    #if(tupleId.tupleIndex < len(self.header.Storage)):
        #start = self.header.offsetOfSlot(tupleId.tupleIndex)
        #end = start + self.header.tupleSize
        #view1 = self.getbuffer()[:start]
        #view2 = self.getbuffer()[end:]
        #self.getbuffer()[start:] = view1
        #self.getbuffer()[:start] = view2
        #self.header.Storage.remove(tupleId.tupleIndex)
    if(tupleId.tupleIndex < len(self.header.Storage)):
        for i in range(tupleId.tupleIndex,len(self.header.Storage)-2):
                start = (i * self.header.tupleSize) + self.header.headerSize()
                end = start + self.header.tupleSize
                start2 = end
                end2 = start2 + self.header.tupleSize
                self.getbuffer()[start:end] = self.getbuffer()[start2:end2]
                view = self.getbuffer()
                if(self.header.hasSlot(i+1)):
                    if(self.header.getSlot(i+1) == 1):
                      self.header.setSlot(i,1)
                    else:
                      self.header.setSlot(i,0)
        self.header.setDirty(True)
    else:
          raise ValueError("The tuple index is not in the page")






  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    #raise NotImplementedError
    #BytesIO(self.buffer).getbuffer()[0:self.header.headerSize()] = self.header.pack()
    self.getbuffer()[0:self.header.headerSize()] = self.header.pack()
    #return BytesIO(self.buffer).getvalue()#self.header.pack()#+ struct.pack('H',self.pageId.fileId) + struct.pack('H',pageId.pageIndex)
    return self.getvalue()


  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    #raise NotImplementedError
    values = cls.headerClass.unpack(BytesIO(buffer).getbuffer())
    return cls(buffer=buffer,header = values,pageId = pageId)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
