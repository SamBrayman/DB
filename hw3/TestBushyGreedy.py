import Database
import numpy
from time import time
from Query.BushyOptimizer import BushyOptimizer
from Query.GreedyOptimizer import GreedyOptimizer
from Catalog.Schema import DBSchema



db = Database.Database()

db.createRelation('one', [('one_one', 'int'), ('one_two', 'int'), ('one_three', 'int')])
db.createRelation('two', [('two_one', 'int'), ('two_two', 'int'), ('two_three', 'int')])
db.createRelation('three', [('three_one', 'int'), ('three_two', 'int'), ('three_three', 'int')])
db.createRelation('four', [('four_one', 'int'), ('four_two', 'int'), ('four_three', 'int')])
db.createRelation('five', [('five_one', 'int'), ('five_two', 'int'), ('five_three', 'int')])
db.createRelation('six', [('six_one', 'int'), ('six_two', 'int'), ('six_three', 'int')])
db.createRelation('seven', [('seven_one', 'int'), ('seven_two', 'int'), ('seven_three', 'int')])
db.createRelation('eight', [('eight_one', 'int'), ('eight_two', 'int'), ('eight_three', 'int')])
db.createRelation('nine', [('nine_one', 'int'), ('nine_two', 'int'), ('nine_three', 'int')])
db.createRelation('ten', [('ten_one', 'int'), ('ten_two', 'int'), ('ten_three', 'int')])
db.createRelation('eleven', [('eleven_one', 'int'), ('eleven_two', 'int'), ('eleven_three', 'int')])
db.createRelation('twelve', [('twelve_one', 'int'), ('twelve_two', 'int'), ('twelve_three', 'int')])

aSchema = db.relationSchema('one')
bSchema = db.relationSchema('two')
cSchema = db.relationSchema('three')
dSchema = db.relationSchema('four')
eSchema = db.relationSchema('five')
fSchema = db.relationSchema('six')
gSchema = db.relationSchema('seven')
hSchema = db.relationSchema('eight')
iSchema = db.relationSchema('nine')
jSchema = db.relationSchema('ten')
kSchema = db.relationSchema('eleven')
lSchema = db.relationSchema('twelve')


for tuple in [aSchema.pack(aSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('one', tuple)
for tuple in [bSchema.pack(bSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('two', tuple)
for tuple in [cSchema.pack(cSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('three', tuple)
for tuple in [dSchema.pack(dSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('four', tuple)
for tuple in [eSchema.pack(eSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('five', tuple)
for tuple in [fSchema.pack(fSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('six', tuple)
for tuple in [gSchema.pack(gSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('seven', tuple)
for tuple in [hSchema.pack(hSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('eight', tuple)
for tuple in [iSchema.pack(iSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('nine', tuple)
for tuple in [jSchema.pack(jSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('ten', tuple)
for tuple in [kSchema.pack(kSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('eleven', tuple)
for tuple in [lSchema.pack(lSchema.instantiate(numpy.random.randint(100), numpy.random.randint(100), numpy.random.randint(100))) for i in range(100)]:
  db.insertTuple('twelve', tuple)


query2 = db.query().fromTable('one').join( \
           db.query().fromTable('two'), \
           method='block-nested-loops', expr='one_one == two_two').finalize()


query4 = db.query().fromTable('one').join( \
           db.query().fromTable('two'), \
           method='block-nested-loops', expr='one_one == two_one').join( \
           db.query().fromTable('three'), \
           method='block-nested-loops', expr='one_one == three_one').join( \
           db.query().fromTable('four'), \
           method='block-nested-loops', expr='one_one == four_one').finalize()

query6 = db.query().fromTable('one').join( \
           db.query().fromTable('two'), \
           method='block-nested-loops', expr='one_one == two_one').join( \
           db.query().fromTable('three'), \
           method='block-nested-loops', expr='one_one == three_one').join( \
           db.query().fromTable('four'), \
           method='block-nested-loops', expr='one_one == four_one').join( \
           db.query().fromTable('five'), \
           method='block-nested-loops', expr='one_one == five_one').join( \
           db.query().fromTable('six'), \
           method='block-nested-loops', expr='one_one == six_one').finalize()

query8 = db.query().fromTable('one').join( \
           db.query().fromTable('two'), \
           method='block-nested-loops', expr='one_one == two_one').join( \
           db.query().fromTable('three'), \
           method='block-nested-loops', expr='one_one == three_one').join( \
           db.query().fromTable('four'), \
           method='block-nested-loops', expr='one_one == four_one').join( \
           db.query().fromTable('five'), \
           method='block-nested-loops', expr='one_one == five_one').join( \
           db.query().fromTable('six'), \
           method='block-nested-loops', expr='one_one == six_one').join( \
           db.query().fromTable('seven'), \
           method='block-nested-loops', expr='one_one == seven_one').join( \
           db.query().fromTable('eight'), \
           method='block-nested-loops', expr='one_one == eight_one').finalize()

query10 = db.query().fromTable('one').join( \
           db.query().fromTable('two'), \
           method='block-nested-loops', expr='one_one == two_one').join( \
           db.query().fromTable('three'), \
           method='block-nested-loops', expr='one_one == three_one').join( \
           db.query().fromTable('four'), \
           method='block-nested-loops', expr='one_one == four_one').join( \
           db.query().fromTable('five'), \
           method='block-nested-loops', expr='one_one == five_one').join( \
           db.query().fromTable('six'), \
           method='block-nested-loops', expr='one_one == six_one').join( \
           db.query().fromTable('seven'), \
           method='block-nested-loops', expr='one_one == seven_one').join( \
           db.query().fromTable('eight'), \
           method='block-nested-loops', expr='one_one == eight_one').join( \
           db.query().fromTable('nine'), \
           method='block-nested-loops', expr='one_one == nine_one').join( \
           db.query().fromTable('ten'), \
           method='block-nested-loops', expr='one_one == ten_one').finalize()

query12 = db.query().fromTable('one').join( \
           db.query().fromTable('two'), \
           method='block-nested-loops', expr='one_one == two_one').join( \
           db.query().fromTable('three'), \
           method='block-nested-loops', expr='one_one == three_one').join( \
           db.query().fromTable('four'), \
           method='block-nested-loops', expr='one_one == four_one').join( \
           db.query().fromTable('five'), \
           method='block-nested-loops', expr='one_one == five_one').join( \
           db.query().fromTable('six'), \
           method='block-nested-loops', expr='one_one == six_one').join( \
           db.query().fromTable('seven'), \
           method='block-nested-loops', expr='one_one == seven_one').join( \
           db.query().fromTable('eight'), \
           method='block-nested-loops', expr='one_one == eight_one').join( \
           db.query().fromTable('nine'), \
           method='block-nested-loops', expr='one_one == nine_one').join( \
           db.query().fromTable('ten'), \
           method='block-nested-loops', expr='one_one == ten_one').join( \
           db.query().fromTable('eleven'), \
           method='block-nested-loops', expr='one_one == eleven_one').join( \
           db.query().fromTable('twelve'), \
           method='block-nested-loops', expr='one_one == twelve_one').finalize()


'''
Bushy Tests
'''

db.optimizer = BushyOptimizer(db)
startTime = time()
db.optimizer.pickJoinOrder(query2)
endTime = time()

print("Bushy Times:")
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 2 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query4)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 4 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query6)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 6 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query8)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 8 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query10)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 10 plans: " + str(endTime - startTime) + "\n")


startTime = time()
db.optimizer.pickJoinOrder(query12)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 12 plans: " + str(endTime - startTime) + "\n")

'''
Greedy Tests
'''


print("Greedy Times:")
db.optimizer = GreedyOptimizer(db)
startTime = time()
db.optimizer.pickJoinOrder(query2)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 2 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query4)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 4 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query6)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 6 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query8)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 8 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query10)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 10 plans: " + str(endTime - startTime) + "\n")

startTime = time()
db.optimizer.pickJoinOrder(query12)
endTime = time()
print("Number of plans considered: " + str(db.optimizer.count))
print("Time 12 plans: " + str(endTime - startTime) + "\n")

db.close()
