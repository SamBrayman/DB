from Storage.Page import Page
from Storage.SlottedPage import SlottedPage
from Storage.File import StorageFile
from Storage.FileManager import FileManager
from Storage.BufferPool import BufferPool
from Catalog.Identifiers import FileId, PageId, TupleId
from Catalog.Schema import DBSchema
from io import BytesIO
import sys
import unittest

# Change this to 'pageClass = SlottedPage' to test the SlottedPage class.
#pageClass = Page
pageClass = SlottedPage
class Hw1PublicTests(unittest.TestCase):
  ###########################################################
  # Page Class Tests
  ###########################################################
  # Utils:
  def makeSchema(self):
    return DBSchema('employee', [('id', 'int'), ('age', 'int')])

  def makeEmployee(self, n):
    schema = self.makeSchema()
    return schema.instantiate(n, 25 + n)

  def makeEmptyPage(self):
    schema = self.makeSchema()
    pId = PageId(FileId(1), 100)
    return pageClass(pageId=pId, buffer=bytes(4096), schema=schema)

  # Tests
  def testPageInsertTuple(self):
    schema = self.makeSchema()
    e1 = self.makeEmployee(1)
    p = self.makeEmptyPage()
    tId = p.insertTuple(schema.pack(e1))
    self.assertIsNotNone(tId)
    #self.assertEqual(tId.pageId.pageIndex,0)

  def testPagePutGetTuple(self):
    # Insert a Tuple
    schema = self.makeSchema()
    e1 = self.makeEmployee(1)
    p = self.makeEmptyPage()
    tId = p.insertTuple(schema.pack(e1))
    self.assertIsNotNone(tId, 'Insert Tuple Returned None!')

    # Get It Back
    e2 = p.getTuple(tId)
    self.assertIsNotNone(e2, 'Get Tuple Returned None!')
    self.assertEqual(e1, schema.unpack(e2), 'Get Tuple Returned an Invalid Tuple!')

    # Update it in place
    e3 = self.makeEmployee(2)
    p.putTuple(tId, schema.pack(e3))

    # Check that the update took effect
    e4 = p.getTuple(tId)
    self.assertEqual(e3, schema.unpack(e4))

  def testPageDeleteTuple(self):
    schema = self.makeSchema()
    e1 = self.makeEmployee(1)
    p = self.makeEmptyPage()
    tId = p.insertTuple(schema.pack(e1))
    p.deleteTuple(tId)
    self.assertIsNone(p.getTuple(tId), 'Deleted tuple is still present in the page!')

  # Stress Tests:
  def testPageInsertMany(self):
    schema = self.makeSchema()
    p = self.makeEmptyPage()
    # Insert 1000 tuples, making sure no errors occur
    for i in range(100):
      e = self.makeEmployee(i)
      tId = p.insertTuple(schema.pack(e))

  def testPageGetMany(self):
    schema = self.makeSchema()
    p = self.makeEmptyPage()
    tids = []
    # Insert 500 tuples, then 'get' them back.
    for i in range(100):
      e = self.makeEmployee(i)
      tId = p.insertTuple(schema.pack(e))
      tids.append(tId)
    for tId in tids:
      if tId is not None:
        e2 = p.getTuple(tId)
        self.assertIsNotNone(e2, 'Get Tuple Returned None!')

  def testPageDeleteMany(self):
    schema = self.makeSchema()
    p = self.makeEmptyPage()
    tids = []
    # Insert 500 tuples, then 'delete' them.
    for i in range(100):
      e = self.makeEmployee(i)
      tId = p.insertTuple(schema.pack(e))
      tids.append(tId)
    for tId in tids:
      if tId is not None:
        p.deleteTuple(tId)

  ###########################################################
  # File Class Tests
  ###########################################################
  # Utils:
  def makeDB(self):
    schema = DBSchema('employee', [('id', 'int'), ('age', 'int')])
    bp = BufferPool()
    fm = FileManager(bufferPool=bp)
    bp.setFileManager(fm)
    return (bp, fm, schema)

  def makePage(self, schema, fId, f,i):
    pId = PageId(fId, i)
    p = pageClass(pageId=pId,  buffer=bytes(f.pageSize()), schema=schema)
    for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(1000)]:
      p.insertTuple(tup)
    return (pId, p)

  # Tests:
  def testFileReadWritePage(self):
    # Initialize database internals, and a new page
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    (pId, p) = self.makePage(schema, fId, f, 0)


    # Write it to the file, check the file size
    f.writePage(p)
    f.flush()
    self.assertEqual(f.numPages(), 1)
    self.assertEqual(f.size(), f.headerSize() + f.pageSize())

    # Read it back in
    pageBuffer = bytearray(f.pageSize())
    pIn1 = f.readPage(pId, pageBuffer)
    view = BytesIO(pIn1)
    #page = pageClass(buffer = view.getbuffer(),pageId = pId, schema =schema  )
    page = pageClass.unpack(buffer=view.getbuffer(),pageId = pId)
    self.assertEqual(page.pageId, pId)
    #print(p.header.usedSpace())
    #print(page.header.usedSpace())
    self.assertEqual(page.header.numTuples(), p.header.numTuples())

    # The tuples in the freshly-read page should be equal
    # to those in the original.
    for (tup1, tup2) in zip(page, p):
      self.assertEqual(tup1, tup2)
    filem.close()

  def testFileAllocatePage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Keep allocating pages, making sure the number of pages in
    # the file is increasing.
    for i in range(10):
      f.allocatePage()
      self.assertEqual(f.numPages(), i+1)
    filem.close()

  def testFileAvailablePage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)

    # Since we aren't adding any data,
    # The available page shouldn't change.
    # Even as we allocate more pages
    initialPage = f.availablePage().pageIndex
    
    for i in range(10):
      f.allocatePage()
      self.assertEqual(f.availablePage().pageIndex, initialPage)
    #print(f.size())
    # Now we fill some pages to check that the available page has changed.
    test = 2000
    for i in range(test):
      f.insertTuple(schema.pack(self.makeEmployee(i)))
      #print(len(f.freePages))
      #print(f.numTuples())
      #print(f.numPages())
    #print(f.size())
    #print(schema.size)
    #print(f.pageSize())
    #for page in f.pages():
        #print(len(page[1].header.Storage))
        #print(page[1].header.pageCapacity)
        #print(page[1].header.freeSpace())
        #print(page[1].header.usedSpace())
        #print(page[1].header.hasFreeTuple())
        #print(page[1].header.numTuples())
        #print(page[1].header.slots)
        #print(page[1].header.numSlotsPos)
        #print(page[1].header.headerSize())
    self.assertNotEqual(f.availablePage().pageIndex, initialPage)#initialPage)
    #self.assertEqual(f.availablePage().pageIndex, 1)#initialPage)
    filem.close()

  def testFileInsertTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert 1000 tuples, checking the files numTuples()
    for i in range(1000):
      f.insertTuple(schema.pack(self.makeEmployee(i)))
      #print(f.numPages())
    self.assertEqual(f.numTuples(), 1000)
    filem.close()

  def testFileDeleteTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    tids = []
    # Insert 1000 tuples, then delete them. File should have 0 tuples.
    for i in range(400):
      tids.append(f.insertTuple(schema.pack(self.makeEmployee(i))))
    for tid in tids:
      f.deleteTuple(tid)
    f.flush()
    self.assertEqual(f.numTuples(), 0)
    filem.close()

  def testFileUpdateTuple(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert and update a single tuple, then check the effect took hold.
    tid = f.insertTuple(schema.pack(self.makeEmployee(1)))
    f.updateTuple(tid, schema.pack(self.makeEmployee(10)))
    for tup in f.tuples():
      self.assertEqual(schema.unpack(tup).id, 10)
    filem.close()

  ###########################################################
  # BufferPool Class Tests
  ###########################################################
  def testBufferPoolHasPage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert a single tuple into a file. The single page
    # should be cached in the buffer pool.
    f.insertTuple(schema.pack(self.makeEmployee(0)))
    self.assertEqual(bufp.hasPage(f.availablePage()), True)
    filem.close()

  def testBufferPoolGetPage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert a single tuple into a file. We should
    # be able to get the single page from the pool.
    f.insertTuple(schema.pack(self.makeEmployee(0)))
    self.assertIsNotNone(bufp.getPage(f.availablePage()))
    filem.close()

  def testBufferPoolDiscardPage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    # Insert a single tuple into a file. Then, discard
    # the single page. The page should no longer be in the pool.
    f.insertTuple(schema.pack(self.makeEmployee(0)))
    pId = f.availablePage()
    #print(pId)
    bufp.discardPage(pId)
    self.assertEqual(bufp.hasPage(pId), False)
    filem.close()

  def testBufferPoolEvictPage(self):
    (bufp, filem, schema) = self.makeDB()
    filem.removeRelation(schema.name)
    filem.createRelation(schema.name, schema)
    (fId, f) = filem.relationFile(schema.name)
    (pId, p) = self.makePage(schema, fId, f, 0)

    # Insert a single tuple into a file. Then evict a page.
    # The single page should no longer be in the pool.
    f.insertTuple(schema.pack(self.makeEmployee(0)))
    pId = f.availablePage()
    bufp.evictPage()
    self.assertEqual(bufp.hasPage(pId), False)
    filem.close()

if __name__ == '__main__':
  unittest.main(argv=[sys.argv[0], '-v'])
