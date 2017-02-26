import io, math, struct

from collections import OrderedDict
from struct      import Struct

from Catalog.Identifiers import PageId, FileId, TupleId
from Catalog.Schema      import DBSchema

import Storage.FileManager

class BufferPool:
  """
  A buffer pool implementation.

  Since the buffer pool is a cache, we do not provide any serialization methods.

  >>> schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
  >>> bp = BufferPool()
  >>> fm = Storage.FileManager.FileManager(bufferPool=bp)
  >>> bp.setFileManager(fm)

  # Check initial buffer pool size
  >>> len(bp.pool.getbuffer()) == bp.poolSize
  True

  """

  # Default to a 10 MB buffer pool.
  defaultPoolSize = 10 * (1 << 20)

  # Buffer pool constructor.
  #
  # REIMPLEMENT this as desired.
  #
  # Constructors keyword arguments, with defaults if not present:
  # pageSize       : the page size to be used with this buffer pool
  # poolSize       : the size of the buffer pool
  def __init__(self, **kwargs):
    self.pageSize     = kwargs.get("pageSize", io.DEFAULT_BUFFER_SIZE)
    self.poolSize     = kwargs.get("poolSize", BufferPool.defaultPoolSize)
    self.pool         = io.BytesIO(b'\x00' * self.poolSize)
    self.map = OrderedDict()
    ####################################################################################
    # DESIGN QUESTION: what other data structures do we need to keep in the buffer pool?
    #self.freeList     = []
    #for i in range(self.numPages):
    #    self.freeList.append(0)


  def setFileManager(self, fileMgr):
    self.fileMgr = fileMgr

  # Basic statistics

  def numPages(self):
    return math.floor(self.poolSize / self.pageSize)

  def numFreePages(self):
    return self.poolSize - (len(self.map)*self.pageSize)
    #raise NotImplementedError

  def size(self):
    return self.poolSize

  def freeSpace(self):
    return self.numFreePages() * self.pageSize

  def usedSpace(self):
    return self.size() - self.freeSpace()


  # Buffer pool operations

  def hasPage(self, pageId):
    if(pageId in self.map):
        return True
    else:
        return False
    #raise NotImplementedError
  
  def getPage(self, pageId):
    if(self.hasPage(pageId)):
        start = pageId.pageIndex * self.pageSize
        end = self.pageSize
        page = self.fileMgr.readPage(pageId,self.fileMgr.pool.getbuffer()[start:end])
        return page
    else:
        try:
            start = pageId.pageIndex * self.pageSize
            end = self.pageSize
            page = self.fileMgr.readPage(pageId,self.fileMgr.bufferPool.getbuffer()[start:end])
            return page
        except:
            raise ValueError("")

    #raise NotImplementedError

  # Removes a page from the page map, returning it to the free 
  # page list without flushing the page to the disk.
  def discardPage(self, pageId):
    if(self.hasPage(pageId)):
        self.map.pop(pageId,None)
    else:
        raise ValueError("This page is not in the buffer pool.")
    #raise NotImplementedError

  def flushPage(self, pageId):
    if(self.hasPage(pageId)):
        page = self.getPage(pageId)
        self.fileMgr.writePage(page)
    else:
        raise ValueError("This page is not in the buffer pool.")
    #raise NotImplementedError

  # Evict using LRU policy. 
  # We implement LRU through the use of an OrderedDict, and by moving pages
  # to the end of the ordering every time it is accessed through getPage()
  def evictPage(self):
    if(self.usedSpace != 0):
        tup = self.map.popitem(last=True)
        page = self.getPage(page)
        if(page.isDirty()):
            self.flushPage(page.pageId)
    else:
        raise ValueError("There are no pages in the buffer pool.")
    #raise NotImplementedError

  # Flushes all dirty pages
  def clear(self):
    for pageId in self.map:
        page = self.getPage(pageId)
        if(page.isDirty()):
            self.flushPage(pageId)
    #raise NotImplementedError

if __name__ == "__main__":
    import doctest
    doctest.testmod()
