from io import BytesIO
import copy, math, struct

from Catalog.Identifiers import TupleId

class PageHeader:
  """
  A base class for page headers, storing bookkeeping information on a page.

  Page headers implement structural equality over their component fields.

  This includes the page's flags (e.g., whether the page is dirty), as well as
  the tuple size for a page, the free space offset within a page and the
  page's capacity.

  This simple page header supports only fixed-size tuples, and a write-once
  implementation of pages by using only a free space offset. That is, the
  free space offset monotonically increases as tuples are inserted into the
  page. Reclaiming space following tuple deletion requires vacuuming (i.e.,
  page reorganization and defragmentation).

  The header size is provided by an explicit method in the base class, and this
  method should be overriden by subclasses to account for the size of any
  additional fields. The exact size of a PageHeader can always be retrieved by
  the 'PageHeader.size' class attribute.

  PageHeaders implement pack and unpack methods to support their storage as
  in-memory buffers and on disk.

  Page headers require the page's backing buffer as a constructor argument.
  This buffer must support Python's buffer protocol, for example as provided
  by a 'memoryview' object. Furthermore, the buffer must be writeable.

  On construction, the page header stores a packed representation of itself
  at the beginning of the page. A page lazily maintains its page header in
  its backing buffer, working primarily with the in-memory representation
  instead. That is, while tuples are inserted and deleted in the page, only
  the Python PageHeader object is directly maintained. It is only when the page
  itself is packed that the page header in the page's buffer is refreshed.

  >>> import io
  >>> buffer = io.BytesIO(bytes(4096))
  >>> ph     = PageHeader(buffer=buffer.getbuffer(), tupleSize=16)
  >>> ph2    = PageHeader.unpack(buffer.getbuffer())
  >>> ph == ph2
  True

  >>> buffer2 = io.BytesIO(bytes(2048))
  >>> ph3     = PageHeader(buffer=buffer2.getbuffer(), tupleSize=16)
  >>> ph == ph3
  False

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

  # First tuple allocated should be at the header boundary
  >>> ph.nextFreeTuple() == ph.headerSize()
  True

  >>> ph.numTuples()
  1

  >>> tuplesToTest = 10
  >>> [ph.nextFreeTuple() for i in range(0,tuplesToTest)]
  [24, 40, 56, 72, 88, 104, 120, 136, 152, 168]

  >>> ph.numTuples() == tuplesToTest+1
  True

  >>> ph.hasFreeTuple()
  True

  # Check space utilization
  >>> ph.usedSpace() == (tuplesToTest+1)*ph.tupleSize
  True

  >>> ph.freeSpace() == 4096 - (ph.headerSize() + ((tuplesToTest+1) * ph.tupleSize))
  True

  >>> remainingTuples = int(ph.freeSpace() / ph.tupleSize)

  # Fill the page.
  >>> [ph.nextFreeTuple() for i in range(0, remainingTuples)] # doctest:+ELLIPSIS
  [184, 200, ..., 4072]

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
  binrepr   = struct.Struct("cHHH")
  size      = binrepr.size

  # Flag bitmasks
  dirtyMask = 0b1   #value 1

  # Page header constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # buffer       : a memoryview on a BytesIO's buffer
  # flags        : a single character byte string indicating the page's status
  # tupleSize    : the tuple size in bytes
  # pageCapacity : the page size in bytes
  def __init__(self, **kwargs):
    buffer               = kwargs.get("buffer", None)
    self.flags           = kwargs.get("flags", b'\x00')
    self.tupleSize       = kwargs.get("tupleSize", None)
    self.pageCapacity    = kwargs.get("pageCapacity", len(buffer))
    self.freeSpaceOffset = kwargs.get("freeSpaceOffset", self.headerSize())    #None
    buffer[0:self.headerSize()] = self.pack();
    #raise NotImplementedError


  # Page header equality operation based on header fields.
  def __eq__(self, other):
    return (    self.flags == other.flags
            and self.tupleSize == other.tupleSize
            and self.pageCapacity == other.pageCapacity
            and self.freeSpaceOffset == other.freeSpaceOffset )

  def __hash__(self):
    return hash((self.flags, self.tupleSize, self.pageCapacity, self.freeSpaceOffset))

  def headerSize(self):
    return PageHeader.size

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

  # Tuple count for the header.
  def numTuples(self):
    return int(self.usedSpace() / self.tupleSize)

  # Returns the space available in the page associated with this header.
  def freeSpace(self):
    return self.pageCapacity - self.freeSpaceOffset

  # Returns the space used in the page associated with this header.
  #???? should this just be tuple used space or can it include pageHeader size?
  #this implementation currently does
  def usedSpace(self):
    return self.freeSpaceOffset - self.headerSize()

  # Returns whether the page has any free space for a tuple.
  def hasFreeTuple(self):
    return self.freeSpace() >= self.tupleSize

  # Returns the page offset of the next free tuple.
  # This should also "allocate" the tuple, such that any subsequent call
  # does not yield the same tupleIndex.
  def nextFreeTuple(self):
    if self.hasFreeTuple():
      self.freeSpaceOffset += self.tupleSize
      return self.freeSpaceOffset - self.tupleSize
    else:
      return None

  # Returns a triple of (tupleIndex, start, end) for the next free tuple.
  # This should cal nextFreeTuple()
  def nextTupleRange(self):
    if self.hasFreeTuple():
      tupleIndex = self.nextFreeTuple()
      start = tupleIndex
      return (tupleIndex, start, start + self.tupleSize)
    else:
      return (None, None, None)

  #Return the (start, end) offsets occupied by the given tuple at given tupleId
  #Return (None, None) if not found
  def getTupleRangeFromId(self, tupleId):
    if self.validTupleId(tupleId):
      start = tupleId.tupleIndex
      return (start, start + self.tupleSize)
    else:
      return (None, None)

  #Check that tupleIndex falls within writable regions of current Page
  def validTupleId(self, tupleId):
    tupleIndex = tupleId.tupleIndex
    if tupleIndex < self.headerSize():
      tupleId.tupleIndex += self.headerSize()
    return (tupleIndex <= (self.freeSpaceOffset - self.tupleSize)) and (tupleIndex >= self.headerSize()) and (tupleIndex % self.tupleSize == 0)

  #Check that the tupleData size matches the page tuple size
  def validTupleData(self, tupleData):
    validTuple = self.tupleSize == len(tupleData)
    return validTuple

  # Returns a binary representation of this page header.
  def pack(self):
    return PageHeader.binrepr.pack(
              self.flags, self.tupleSize,
              self.freeSpaceOffset, self.pageCapacity)

  # Constructs a page header object from a binary representation held in a byte string.
  @classmethod
  def unpack(cls, buffer):
    values = PageHeader.binrepr.unpack_from(buffer)
    if len(values) == 4:
      return cls(buffer=buffer, flags=values[0], tupleSize=values[1],
                 freeSpaceOffset=values[2], pageCapacity=values[3])


class Page(BytesIO):
  """
  A page class, representing a unit of storage for database tuples.

  A page includes a page identifier, and a page header containing metadata
  about the state of the page (e.g., its free space offset).

  Our page class inherits from an io.BytesIO, providing it an implementation
  of a in-memory binary stream.

  The page constructor requires a byte buffer in which we can store tuples.
  The user has the responsibility for constructing a suitable buffer, for
  example with Python's 'bytes()' builtin.

  The page also provides several methods to retrieve and modify its contents
  based on a tuple identifier, and where relevant, tuple data represented as
  an immutable sequence of bytes.

  The page's pack and unpack methods can be used to obtain a byte sequence
  capturing both the page header and tuple data information for storage on disk.
  The page's pack method is responsible for refreshing the in-buffer representation
  of the page header prior to return the entire page as a byte sequence.
  Currently this byte-oriented representation does not capture the page identifier.
  This is left to the file structure to inject into the page when constructing
  this Python object.

  This class imposes no restriction on the page size.

  >>> from Catalog.Identifiers import FileId, PageId, TupleId
  >>> from Catalog.Schema      import DBSchema

  # Test harness setup.
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> pId    = PageId(FileId(1), 100)
  >>> p      = Page(pageId=pId, buffer=bytes(4096), schema=schema)

  # Test page packing and unpacking
  >>> len(p.pack())
  4096
  >>> p2 = Page.unpack(pId, p.pack())
  >>> p.pageId == p2.pageId
  True
  >>> p.header == p2.header
  True

  # Create and insert a tuple
  >>> e1 = schema.instantiate(1,25)
  >>> tId = p.insertTuple(schema.pack(e1))

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

  # Add some more tuples
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  # Check number of tuples in page
  >>> p.header.numTuples()
  11


  # Test iterator
  >>> [schema.unpack(tup).age for tup in p]
  [28, 20, 22, 24, 26, 28, 30, 32, 34, 36, 38]

  # Test clearing of first tuple
  >>> tId = TupleId(p.pageId, 0 + p.header.headerSize())
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


  # Check that the page's data segment has been compacted after the remove.
  >>> p.header.usedSpace() == (sizeBeforeRemove - p.header.tupleSize)
  True

  # More delete tests
  >>> tId = TupleId(p.pageId, p.header.headerSize() + p.header.tupleSize * 4)
  >>> p.deleteTuple(tId)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 30, 32, 34, 36, 38]

  >>> p.clearTuple(tId)

  >>> schema.unpack(p.getTuple(tId))
  employee(id=0, age=0)

  >>> [schema.unpack(tup).age for tup in p]
  [20, 22, 24, 26, 0, 32, 34, 36, 38]

  """

  headerClass = PageHeader

  # Page constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # buffer       : a byte string of initial page contents.
  # pageId       : a PageId instance identifying this page.
  # header       : a PageHeader instance.
  # schema       : the schema for tuples to be stored in the page.
  # Also, any keyword arguments needed to construct a PageHeader.
  def __init__(self, **kwargs):
    buffer = kwargs.get("buffer", None)
    if buffer:
      BytesIO.__init__(self, buffer)
      self.pageId = kwargs.get("pageId", None)
      header      = kwargs.get("header", None)
      schema      = kwargs.get("schema", None)

      if self.pageId and header:   #if both are not null
        self.header = header
      elif self.pageId:
        self.header = self.initializeHeader(**kwargs)
      else:
        raise ValueError("No page identifier provided to page constructor.")



    else:
      raise ValueError("No backing buffer provided to page constructor.")


  # Header constructor. This can be overridden by subclasses.
  def initializeHeader(self, **kwargs):
    schema = kwargs.get("schema", None)
    if schema:
      return PageHeader(buffer=self.getbuffer(), tupleSize=schema.size)
    else:
      raise ValueError("No schema provided when constructing a page.")

  # Iterator
  def __iter__(self):
    self.iterTupleIdx = 0 + self.header.headerSize()
    return self

  def __next__(self):
    t = self.getTuple(TupleId(self.pageId, self.iterTupleIdx))
    if t:
      self.iterTupleIdx += self.header.tupleSize
      return t
    else:
      raise StopIteration

  # Dirty bit accessors
  def isDirty(self):
    return self.header.isDirty()

  def setDirty(self, dirty):
    self.header.setDirty(dirty)

  # Tuple accessor methods

  # Returns a byte string representing a packed tuple for the given tuple id.
  def getTuple(self, tupleId):
    if self.header and tupleId:
      #need start and end range to access tuple
      (start, end) = self.header.getTupleRangeFromId(tupleId)
      if start and end:
        return self.getbuffer()[start:end]
      #else:
        #raise ValueError("Tuple index is out of bounds of current page.")
      #return None

  #???? what is someone tries to put tuple at index that breaks
  #contiguity-  right now it is set
  #???? clarify validTuple, tupleIndex
  #Do we need to be doing these safety checks
  # Updates the (packed) tuple at the given tuple id.
  def putTuple(self, tupleId, tupleData):
    if self.header and tupleId and tupleData and self.header.validTupleData(tupleData):
      #getTupleRangeFromId will check valid tupleIndex
      (start, end) = self.header.getTupleRangeFromId(tupleId)
      if start and end:
        self.getbuffer()[start:end] = tupleData
        self.setDirty(True)
      if not self.header.validTupleId(tupleId):
        raise ValueError("Tuple index is out of bounds of current page.")


  # Adds a packed tuple to the page. Returns the tuple id of the newly added tuple.
  def insertTuple(self, tupleData):
    if self.header and tupleData and self.header.validTupleData(tupleData):
      #nextTupleRange checks if page has another tuple opening
      (tupleIndex, start, end) = self.header.nextTupleRange()
      if tupleIndex and start and end:
        self.getbuffer()[start:end] = tupleData
        self.setDirty(True)

        index = 0
        return TupleId(self.pageId, tupleIndex)
      else:
        #no free tuple space
        raise ValueError("Tuple cannot be inserted because page is full.")


  # Zeroes out the contents of the tuple at the given tuple id.
  def clearTuple(self, tupleId):
    #raise NotImplementedError
    '''if self.header and tupleId:
      (start, end) = self.header.getTupleRangeFromId(tupleId)
      if start and end:
        zeroedOutValue = self.header.tupleSize * b'\x00'
        self.getbuffer()[start:end] = zeroedOutValue
        self.setDirty(True)
      else:
        raise ValueError("Not a valid tupleId.")'''
    if self.header and tupleId:
      start = tupleId.tupleIndex
      #
      #REMEMBER TO DO THIS SHIT BELOW!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      #REMEMBER TO DO THIS SHIT BELOW!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      #REMEMBER TO DO THIS SHIT BELOW!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      #REMEMBER TO DO THIS SHIT BELOW!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      #REMEMBER TO DO THIS SHIT BELOW!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
      #
      #if (start < self.header.headerSize()):
        #start += self.header.headerSize()
      if (start < self.header.headerSize()) or (start > self.header.freeSpaceOffset) or (start % self.header.tupleSize != 0):
        raise ValueError("Not a valid tupleId.")
      else:
        zeroedOutValue = self.header.tupleSize * b'\x00'
        self.getbuffer()[start:start + self.header.tupleSize] = zeroedOutValue
        self.setDirty(True)


  # Removes the tuple at the given tuple id, shifting subsequent tuples.
  def deleteTuple(self, tupleId):
    if self.header and tupleId:
      (start, end) = self.header.getTupleRangeFromId(tupleId)
      if start and end:
        for index in range(end, self.header.freeSpaceOffset + self.header.tupleSize, self.header.tupleSize):
          self.getbuffer()[(index - self.header.tupleSize):index] = self.getvalue()[index : index + self.header.tupleSize]

        self.header.freeSpaceOffset -= self.header.tupleSize;
        self.setDirty(True);

        #for index in range(0, self.header.freeSpaceOffset, self.header.tupleSize):
          #print(self.getvalue()[index : index + self.header.tupleSize])


      #else:
        #raise ValueError("Not a valid tupleId, tuple cannot be deleted.")


  # Returns a binary representation of this page.
  # This should refresh the binary representation of the page header contained
  # within the page by packing the header in place.
  def pack(self):
    if self.header:
      self.getbuffer()[0:self.header.headerSize()] = self.header.pack()
      return self.getvalue()

  # Creates a Page instance from the binary representation held in the buffer.
  # The pageId of the newly constructed Page instance is given as an argument.
  @classmethod
  def unpack(cls, pageId, buffer):
    #need to recreate, unpack header and buffer (page)
    header = cls.headerClass.unpack(BytesIO(buffer).getbuffer())
    return cls(buffer=buffer, pageId=pageId, header=header)



if __name__ == "__main__":
    import doctest
    doctest.testmod()

    
