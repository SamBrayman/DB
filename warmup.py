import struct

# Construct objects w/ fields corresponding to columns.
# Store fields using the appropriate representation:
  # TEXT => bytes
  # DATE => bytes
  # INTEGER => int
  # FLOAT => float

class Lineitem(object):
  # The format string, for use with the struct module.
  fmt = "iiiiffff1s1ssss26s10s45s"
  # Initialize a lineitem object.
  # Arguments are strings that correspond to the columns of the tuple.
  # Feel free to use __new__ instead.
  # (e.g., if you decide to inherit from an immutable class).
  def __init__(self, *args):   
    self.l_orderkey = int(args[0])
    self.l_partkey = int(args[1])
    self.l_suppkey = int(args[2])
    self.l_linenumber = int(args[3])
    self.l_quantity = float(args[4])
    self.l_extendedprice = float(args[5])
    self.l_discount = args[6]
    self.l_tax = args[7]
    self.l_returnflag = args[8]
    self.l_linestatus = args[9]
    self.l_shipdate = args[10]
    self.l_commitdate = args[11]
    self.l_receiptdate = args[12]
    self.l_shipinstruct = args[13]
    self.l_shipmode = args[14]
    self.l_comment = args[15]
    fmt = "iiiiffff1s1ssss26s10s45s"
  # Pack this lineitem object into a bytes object.
  def pack(self):

    struct.pack(fmt,self.l_orderkey, self.l_partkey,self.l_suppkey,self.l_linenumber,self.l_quantity,self.l_extendedprice,self.l_discount,self.l_tax,self.l_returnflag,self.l_linestatus,self.l_shipdate,self.l_commitdate,self.l_receiptdate,self.l_shipinstruct,self.l_shipmode,self.l_comment)
    #raise NotImplementedError()

  # Construct a lineitem object from a bytes object.
  @classmethod
  def unpack(cls, byts):
    fmt = "iiiiffff1s1ssss26s10s45s"
    data = struct.unpack(fmt,byts)
    return cls(*data)
    #raise NotImplementedError()

    byts = struct.pack(fmt, self.l_orderkey, self.l_partkey, self.l_suppkey, self.l_linenumber,
    self.l_quantity, self.l_extendedprice, self.l_discount, self.l_tax, self.l_returnflag,
    self.l_linestatus, self.l_shipdate, self.l_commitdate, self.l_receiptdate, self.l_shipinstruct,
    self.l_shipmode, self.l_comment)
    return byts
  # Construct a lineitem object from a bytes object.
  @classmethod
  def unpack(cls, byts):
    fmt = "iiiiffff1s1ssss26s10s45s"
    unpacked = struct.unpack(fmt, byts)
    return LineItem(unpacked)


  # Return the size of the packed representation.
  # Do not change.
  @classmethod
  def byteSize(cls):
    return struct.calcsize(cls.fmt)

    
class Orders(object):
  # The format string, for use with the struct module.
  
  # Initialize an orders object.
  fmt = "ii1sfs15s15si79s"
  # Arguments are strings that correspond to the columns of the tuple.
  # Feel free to use __new__ instead.
  # (e.g., if you decide to inherit from an immutable class).
  def __init__(self, *args):
    self.o_orderkey = int(args[0])
    self.o_custkey = int(args[1])
    self.o_orderstatus = args[2]
    self.o_totalprice = args[3]
    self.o_orderdate = args[4]
    self.o_orderpirority = args[5]
    self.o_orderdate = args[4]
    self.o_orderpriority = args[5]
    self.o_clerk = args[6]
    self.o_shippriority = int(args[7])
    self.o_comment = args[8]
    fmt = "ii1sfs15s15si79s"
  # Pack this orders object into a bytes object.

  def pack(self): 
    fmt = "ii1sfs15s15si79s"
    return struct.pack(fmt,self.o_orderkey,self.o_custkey,self.o_orderstatus,self.o_totalprice,self.o_orderdate,self.o_orderpirority,self.o_clerk,self.o_shippriority,self.o_comment)
    #raise NotImplementedError()

  # Construct an orders object from a bytes object.
  @classmethod
  def unpack(cls, byts):
    data = struct.unpack(fmt,byts)
    return cls(*data)
    #raise NotImplementedError()

  def pack(self):
    fmt = "ii1sfs15s15si79s"
    byts = struct.pack(fmt, self.o_orderkey, self.o_custkey, self.o_orderstatus, float(self.o_totalprice), 
      self.o_orderdate, self.o_orderpriority, self.o_clerk,int(self.o_shippriority), self.o_comment)
    return byts
  # Construct an orders object from a bytes object.
  @classmethod
  def unpack(cls, byts):
    fmt = "ii1sfs15s15si79s"
    unpacked = struct.unpack(fmt, byts)
    return cls(*unpacked)

  
  # Return the size of the packed representation.
  # Do not change.
  @classmethod
  def byteSize(cls):
    return struct.calcsize(cls.fmt)

# Return a list of 'cls' objects.
# Assuming 'cls' can be constructed from the raw string fields.
def readCsvFile(inPath, cls, delim='|'):
  lst = []
  with open(inPath, 'r') as f:
    for line in f:
      fields = line.strip().split(delim)
      lst.append(cls(*fields))
  return lst

# Write the list of objects to the file in packed form.
# Each object provides a 'pack' method for conversion to bytes.
def writeBinaryFile(outPath, lst):
    with open(outPath,'w') as target:
      for item in lst:
        data = item.pack()
        target.write(data)
        target.write("\n")
    target.close()
  #raise NotImplementedError()

# Read the binary file, and return a list of 'cls' objects.
# 'cls' provicdes 'byteSize' and 'unpack' methods for reading and conversion.
def readBinaryFile(inPath, cls):
    lst = []
    with open(inPath,'r') as target:
      for line in target:
        lst.append(cls.unpack(line.split()))
    return lst
  #raise NotImplementedError()
