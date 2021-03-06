import io, math, os, os.path, pickle, struct
from struct import Struct
from queue import Queue
from collections import OrderedDict
from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema
from Storage.Page        import PageHeader, Page
from Storage.SlottedPage import SlottedPageHeader, SlottedPage


class FileHeader:
  """
  A file header class, containing a page size and a schema for the data
  entries stored in the file.

  Our file header object also keeps its own binary representation per instance
  rather than at the class level, since each file may have a variable length schema.
  The binary representation is a struct, with three components in its format string:
  i.   header length
  ii.  page size
  iii. a JSON-serialized schema (from DBSchema.packSchema)

  >>> schema = DBSchema('employee', [('id', 'int'), ('dob', 'char(10)'), ('salary', 'int')])
  >>> fh = FileHeader(pageSize=io.DEFAULT_BUFFER_SIZE, pageClass=SlottedPage, schema=schema)
  >>> b = fh.pack()
  >>> fh2 = FileHeader.unpack(b)
  >>> fh.pageSize == fh2.pageSize
  True

  >>> fh.schema.schema() == fh2.schema.schema()
  True

  ## Test the file header's ability to be written to, and read from a Python file object.
  >>> f1 = open('test.header', 'wb')
  >>> fh.toFile(f1)
  >>> f1.flush(); f1.close()

  >>> f2 = open('test.header', 'r+b')
  >>> fh3 = FileHeader.fromFile(f2)
  >>> fh.pageSize == fh3.pageSize \
      and fh.pageClass == fh3.pageClass \
      and fh.schema.schema() == fh3.schema.schema()
  True

  >>> os.remove('test.header')
  """

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    else:
      pageSize    = kwargs.get("pageSize", None)

      pageClass   = kwargs.get("pageClass", None)
      schema      = kwargs.get("schema", None)

      if pageSize and pageClass and schema:
        pageClassLen   = len(pickle.dumps(pageClass))
        schemaDescLen  = len(schema.packSchema())
        self.binrepr   = Struct("HHHH"+str(pageClassLen)+"s"+str(schemaDescLen)+"s")
        self.size      = self.binrepr.size

        self.pageSize  = pageSize
        self.pageClass = pageClass
        self.schema    = schema

      else:
        raise ValueError("Invalid file header constructor arguments")

  def fromOther(self, other):
    self.binrepr   = other.binrepr
    self.size      = other.size
    self.pageSize  = other.pageSize
    self.pageClass = other.pageClass
    self.schema    = other.schema

  def pack(self):
    if self.binrepr and self.pageSize and self.schema:
      packedPageClass = pickle.dumps(self.pageClass)
      packedSchema    = self.schema.packSchema()
      return self.binrepr.pack(self.size, self.pageSize, \
              len(packedPageClass), len(packedSchema), \
              packedPageClass, packedSchema)

  @classmethod
  def binrepr(cls, buffer):
    lenStruct = Struct("HHHH")
    (headerLen, _, pageClassLen, schemaDescLen) = lenStruct.unpack_from(buffer)
    if headerLen > 0 and pageClassLen > 0 and schemaDescLen > 0:
      return Struct("HHHH"+str(pageClassLen)+"s"+str(schemaDescLen)+"s")
    else:
      raise ValueError("Invalid header length read from storage file header")

  @classmethod
  def unpack(cls, buffer):
    brepr  = cls.binrepr(buffer)
    values = brepr.unpack_from(buffer)
    if len(values) == 6:
      pageClass = pickle.loads(values[4])
      schema    = DBSchema.unpackSchema(values[5])
      return FileHeader(pageSize=values[1], pageClass=pageClass, schema=schema)

  def toFile(self, f):
    pos = f.tell()
    if pos == 0:
      f.write(self.pack())
    else:
      raise ValueError("Cannot write file header, file positioned beyond its start.")

  @classmethod
  def fromFile(cls, f):
    pos = f.tell()
    if pos == 0:
      lenStruct = Struct("H")
      headerLen = lenStruct.unpack_from(f.peek(lenStruct.size))[0]
      if headerLen > 0:
        buffer = f.read(headerLen)
        return FileHeader.unpack(buffer)
      else:
        raise ValueError("Invalid header length read from storage file header")
    else:
      raise ValueError("Cannot read file header, file positioned beyond its start.")



class StorageFile:
  """
  A storage file implementation, as a base class for all database files.

  All storage files have a file identifier, a file path, a file header and a handle
  to a file object as metadata.

  This implementation supports a readPage() and writePage() method, enabling I/O
  for specific pages to the backing file. Allocation of new pages is handled by the
  underlying file system (i.e. simply write the desired page, and the file system
  will grow the backing file by the desired amount).

  Storage files may also serialize their metadata using the pack() and unpack(),
  allowing their metadata to be written to disk when persisting the database catalog.

  >>> import shutil, Storage.BufferPool, Storage.FileManager
  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = Storage.BufferPool.BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Create a relation for the given schema
  >>> fm.createRelation(schema.name, schema)

  # Below 'f' is a StorageFile object returned by the FileManager
  >>> (fId, f) = fm.relationFile(schema.name)

  # Check initial file status
  >>> f.numPages() == 0
  True

  # There should be a valid free page data structure in the file.
  >>> f.freePages is not None
  True

  # The first available page should be at page offset 0.
  >>> f.availablePage().pageIndex
  0

  # Create a pair of pages.
  >>> pId  = PageId(fId, 0)
  >>> pId1 = PageId(fId, 1)
  >>> p    = Page(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
  >>> p1   = Page(pageId=pId1, buffer=bytes(f.pageSize()), schema=schema)

  # Populate pages
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(10)]:
  ...    _ = p.insertTuple(tup)
  ...

  >>> for tup in [schema.pack(schema.instantiate(i, i+20)) for i in range(10, 20)]:
  ...    _ = p1.insertTuple(tup)
  ...

  # Write out pages and sync to disk.
  >>> f.writePage(p)
  >>> f.writePage(p1)
  >>> f.flush()

  # Check the number of pages, and the file size.
  >>> f.numPages() == 2
  True

  >>> f.size() == (f.headerSize() + f.pageSize() * 2)
  True

  #>>> pId2 = PageId(fId, 2)
  #>>> p2    = Page(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
  #>>> for tup in [schema.pack(schema.instantiate(i, i+20)) for i in range(10, 20)]:
  #...    _ = p1.insertTuple(tup)
  #...
  #>>> f.writePage(p2)
  #>>> f.numPages() == 2
  #True

  # Read pages in reverse order testing offset and page index.
  >>> pageBuffer = bytearray(f.pageSize())
  >>> pIn1 = f.readPage(pId1, pageBuffer)
  >>> page2 = f.pageClass().unpack(buffer = io.BytesIO(pIn1).getbuffer(),pageId = pId1)
  >>> page2.pageId == pId1
  True

  >>> f.pageOffset(page2.pageId) == f.header.size + f.pageSize()
  True

  >>> pIn = f.readPage(pId, pageBuffer)
  >>> page2 = f.pageClass().unpack(buffer = io.BytesIO(pIn1).getbuffer(),pageId = pId)
  >>> page2.pageId == pId
  True

  >>> f.pageOffset(page2.pageId) == f.header.size
  True

  # Test page header iterator
  >>> [p[1].usedSpace() for p in f.headers()]
  [80, 80]

  # Test page iterator
  >>> [p[1].pageId.pageIndex for p in f.pages()]
  [0, 1]

  # Test tuple iterator
  >>> [schema.unpack(tup).id for tup in f.tuples()] == list(range(20))
  True

  # Check buffer pool utilization
  >>> (bp.numPages() - bp.numFreePages()) == 2
  True

  """

  # Change this to the Page class if you want contiguous page storage in the file.
  #defaultPageClass = Page
  defaultPageClass = SlottedPage
  # StorageFile constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments:
  # bufferPool   : a buffer pool instance.
  # fileId       : a PageId instance identifying this page.
  # filePath     : a PageHeader instance.
  # mode         : the file open mode. Can be one of 'create', 'update', 'truncate'.
  # Also, any keyword arguments needed to construct a FileHeader.
  def __init__(self, **kwargs):
    self.bufferPool = kwargs.get("bufferPool", None)
    if self.bufferPool is None:
      raise ValueError("No buffer pool found when initializing a storage file")

    pageSize = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    pageClass = kwargs.get("pageClass", StorageFile.defaultPageClass)
    schema = kwargs.get("schema", None)
    self.mode = kwargs.get("mode", None)
    self.fileId    = kwargs.get("fileId", None)
    self.filePath  = kwargs.get("filePath", None)
    if(self.fileId is None or self.filePath is None):
        raise ValueError("Invalid input arguments.")
    ######################################################################################
    # DESIGN QUESTION: how do you initialize these?
    # The file should be opened depending on the desired mode of operation.
    # The file header may come from the file contents (i.e., if the file already exists),
    # otherwise it should be created from scratch.

    if(self.mode == 'create' or self.mode == 'truncate'):
            self.header = FileHeader(pageSize=pageSize,pageClass=pageClass,schema = schema)
            self.file = open(self.filePath,'w+b')
            self.file.write(self.header.pack())
            self.file.flush()
    elif(self.mode == 'update'):
            self.file = open(self.filePath,'r+b')
            self.header = FileHeader.fromFile(self.file)

    #elif(self.mode == 'truncate'):
    #        self.file.flush()
    else:
            raise ValueError("No mode was given.")


    ######################################################################################
    # DESIGN QUESTION: what data structure do you use to keep track of the free pages?

    self.freePages = OrderedDict()



    #Fill queue
    #raise NotImplementedError


  # File control
  def flush(self):
    self.file.flush()

  def close(self):
    if not self.file.closed:
      self.file.close()

  # Storage file helpers
  def pageId(self, pageIndex):
    return PageId(self.fileId, pageIndex)

  def schema(self):
    return self.header.schema

  def pageSize(self):
    return self.header.pageSize

  def numTuples(self):
      #return self.header.numTuples
      #self.bufferPool.clear()
      count = 0
      for pageId in self.bufferPool.map:
        page = self.bufferPool.getPage(pageId)
        #view = io.BytesIO(page[1])
        view = io.BytesIO(page)
        page2 = self.pageClass().unpack(buffer=view.getbuffer(),pageId= pageId)
        count += page2.header.numTuples()
       #print("????????????????????????????????????????????????????????????????????")
       #print(self.schema().size)
        #count += page[1].header.usedSpace()/self.schema().size
      #for page in self.pages():
      #    count +=1
      return count

  def pageClass(self):
    return self.header.pageClass

  def size(self):
    return os.path.getsize(self.filePath)

  def headerSize(self):
      if(self.header is not None):
        return self.header.size
      else:
        return 0
    #raise NotImplementedError

  def numPages(self):
    return math.floor((self.size() - self.headerSize()) / self.pageSize())
    #return self.bufferPool.numPages()
    #raise NotImplementedError

  # Returns the offset in the file corresponding to the given page id.
  # Notice this assumes the header is written before the first page,
  # and is not part of the first page itself.
  def pageOffset(self, pageId):
    return self.headerSize() + (self.pageSize() * pageId.pageIndex)

  # Returns whether the given page id is valid for this file.
  def validPageId(self, pageId):

    return pageId.fileId == self.fileId and pageId.pageIndex < self.numPages()


  # Page header operations

  # Reads a page header from disk.
  def readPageHeader(self, pageId):
    #self.file = open(self.filePath,'r+b')
    if(self.validPageId(pageId) and self.header):
        start = self.pageOffset(pageId)
        self.file.seek(start)
        end = start + self.pageSize()
        view = bytearray(self.pageSize())
        #headerPart = line below this
        self.file.readinto(view)
        buff = io.BytesIO(view)
        #bytesObj = io.BytesIO(headerPart)
        return self.pageClass().headerClass.unpack(buffer = buff.getbuffer())
    else:
        raise ValueError("The pageId was invalid.")
    #raise NotImplementedError
    #self.file.close()

  # Writes a page header to disk.
  # The page must already exist, that is we cannot extend the file with only a page header.
  def writePageHeader(self, page):
    if(page):
        self.writePage(page)
    else:
        raise ValueError("The page was invalid.")
    #raise NotImplementedError


  # Page operations

  def readPage(self, pageId, page):
    #self.file = open(self.filePath,'r+b')
    #self.file.seek(0)

    if self.validPageId(pageId):
      start = self.pageOffset(pageId)
      end = start + self.pageSize()
      self.file.seek(start)
      #view = self.file.readinto(page)
      view2 = bytearray(self.pageSize())
      self.file.readinto(view2)
      return view2
    else:
        raise ValueError("The page was invalid.")
    #raise NotImplementedError

  def writePage(self, page):
    #self.file = open(self.filePath,'r+b')
    if(page):
        self.file.seek(0)
        #print(((page.pageId.pageIndex) * self.pageSize()) + self.headerSize())
        self.file.seek(((page.pageId.pageIndex) * self.pageSize()) + self.headerSize())
        self.file.write(page.pack())
        self.file.flush()
    else:
        raise ValueError("The page is invalid.")
    #raise NotImplementedError
    #self.file.close()
  # Adds a new page to the file by writing past its end.
  def allocatePage(self):
      pageId = self.pageId(self.numPages())
      #if(pageId.pageIndex  > 0):
      #    pageId.pageIndex = 20

      page = self.pageClass()(pageId = pageId, schema = self.schema(), buffer = bytes(self.pageSize()))
      self.writePage(page)

      #since it was just allocated it has free space
      self.freePages[page.pageId] = None
      self.file.flush()
      #self.bufferPool.discardPage(pageId)
      #self.bufferPool.insertPage(page)
      return page
    #raise NotImplementedError

  # Returns the page id of the first page with available space.
  def availablePage(self):
    if len(self.freePages) != 0:
        return list(self.freePages.keys())[0]
    else:
        page = self.allocatePage()
        #self.freePages.put(page.pageId)
        return page.pageId
    #raise NotImplementedError


  # Tuple operations
  # Inserts the given tuple to the first available page.
  def insertTuple(self, tupleData):
    pageId = self.availablePage()
    page = self.bufferPool.getPage(pageId)  #this is where invalid pageId comes
    view = io.BytesIO(page)
    page2 = self.pageClass().unpack(buffer=view.getbuffer(),pageId= pageId)
    if(page2.header.hasFreeTuple()):
      tupleId =  page2.insertTuple(tupleData)
    else:
      raise ValueError("This page id is invalid.")
    self.bufferPool.discardPage(pageId)
    self.bufferPool.insertPage(page2)

    #page.insert(tupleData)
    #self.writePage(page)
    #self.bufferpool.getPage(page.pageId)
    #if(~(page.hasFreeTuple())):
     #   self.freePages.get()
    #raise NotImplementedError
    if page2.header.hasFreeTuple():
        self.freePages[page2.pageId] = None
    else:
        #self.storageFile.bufferPool.getPage(pId)
        #If no free space anymore, need to remove from freePages
        self.freePages.pop(page2.pageId)
    return tupleId


  # Removes the tuple by its id, tracking if the page is now free
  def deleteTuple(self, tupleId):
    if(self.validPageId(tupleId.pageId)):
        pageId = tupleId.pageId
        #page = self.pageClass()(pageId = pageId,schema = self.schema(),buffer = bytes(self.pageSize()))
        page = self.bufferPool.getPage(pageId)
        self.bufferPool.discardPage(pageId)
        view = io.BytesIO(page)
        page2 = self.pageClass().unpack(buffer=view.getbuffer(),pageId= pageId)
        notfull = page2.header.hasFreeTuple()
        page2.deleteTuple(tupleId)
        self.bufferPool.insertPage(page2)
        if(page2.header.numTuples() == 0):
            self.bufferPool.discardPage(pageId)
        self.writePage(page2)
        self.bufferPool.clear()
        if(notfull):
    #if page.header.hasFreeTuple():
         self.freePages[pageId] = None
    else:
        raise ValueError("This is not a valid tupleID.")
    #raise NotImplementedError

  # Updates the tuple by id
  def updateTuple(self, tupleId, tupleData):
    if(self.validPageId(tupleId.pageId)):
        pageId = tupleId.pageId
        page = self.bufferPool.getPage(pageId)
        view = io.BytesIO(page)
        page2 = self.pageClass().unpack(buffer=view.getbuffer(),pageId= pageId)
        page2.putTuple(tupleId,tupleData)
        self.bufferPool.insertPage(page2)
    else:
        raise ValueError("This is not a valid tupleID.")
      #raise NotImplementedError


  # Iterators
  # Page header iterator
  def headers(self):
    return self.FileHeaderIterator(self)

  # Page iterator, using the buffer pool
  def pages(self):
    return self.FilePageIterator(self)

  # Unbuffered page iterator.
  # Use with care, direct pages are not authoritative if the page is present in the buffer pool.
  def directPages(self):
    return self.FileDirectPageIterator(self)

  # Tuple iterator
  def tuples(self):
    return self.FileTupleIterator(self)


  # Iterator class implementations
  class FileHeaderIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        if self.storageFile.bufferPool.hasPage(pId):
          view = self.storageFile.bufferPool.getPage(pId)
          page = io.BytesIO(view)
          page2 = self.storageFile.pageClass().unpack(buffer = page.getbuffer(),pageId = pId)
          return (pId, page2.header)
        else:
          return (pId, self.storageFile.readPageHeader(pId))
      else:
        raise StopIteration

  class FilePageIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        view = self.storageFile.bufferPool.getPage(pId)
        page = io.BytesIO(view)
        page2 = self.storageFile.pageClass().unpack(buffer = page.getbuffer(),pageId = pId)
        return (pId, page2)
      else:
        raise StopIteration

  class FileDirectPageIterator:
    def __init__(self, storageFile):
      self.currentPageIdx = 0
      self.storageFile    = storageFile
      self.buffer         = bytearray(storageFile.pageSize())

    def __iter__(self):
      return self

    def __next__(self):
      pId = self.storageFile.pageId(self.currentPageIdx)
      if self.storageFile.validPageId(pId):
        self.currentPageIdx += 1
        return (pId, self.storageFile.readPage(pId, self.buffer))
      else:
        raise StopIteration

  class FileTupleIterator:
    def __init__(self, storageFile):
      self.storageFile     = storageFile
      self.pageIterator    = storageFile.pages()
      self.nextPage()

    def __iter__(self):
      return self

    def __next__(self):
      if self.pageIterator is not None:
        while self.tupleIterator is not None:
          try:
            return next(self.tupleIterator)
          except StopIteration:
            self.nextPage()

      if self.pageIterator is None:
        raise StopIteration

    def nextPage(self):
      try:
        self.currentPage   = next(self.pageIterator)[1]
      except StopIteration:
        self.pageIterator  = None
        self.tupleIterator = None
      else:
        self.tupleIterator = iter(self.currentPage)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
