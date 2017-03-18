import math, random, sys
from collections import deque
from itertools import *
from Catalog.Schema  import DBSchema

from Query.Operators.TableScan import TableScan
from Query.Operators.Select    import Select
from Query.Operators.Project   import Project
from Query.Operators.Union     import Union
from Query.Operators.Join      import Join
from Query.Operators.GroupBy   import GroupBy
from Query.Operators.Sort      import Sort

class Plan:
  """
  A data structure implementing query plans.

  Query plans are tree data structures whose nodes are objects
  inheriting from the Query.Operator class.

  Our Query.Plan class tracks the root of the plan tree,
  and provides basic accessors such as the ability to
  retrieve the relations accessed by the query, the query's
  output schema, and plan pretty printing facilities.

  Plan instances delegate their iterator to the root operator,
  enabling direct iteration over query results.

  Plan instances should use the 'prepare' method prior to
  iteration (as done with Database.processQuery), to initialize
  all operators contained in the plan.
  """

  def __init__(self, **kwargs):
    other = kwargs.get("other", None)
    if other:
      self.fromOther(other)

    elif "root" in kwargs:
      self.root = kwargs["root"]
      self.sampleCardinality = 0

    else:
      raise ValueError("No root operator specified for query plan")

  def fromOther(self):
    self.root = other.root
    self.cardinality = other.cardinality

  # Returns the root operator in the query plan
  def root(self):
    return self.root

  # Returns the query result schema.
  def schema(self):
    return self.root.schema()

  # Returns the relations used by the query.
  def relations(self):
    return [op.relationId() for (_,op) in self.flatten() if isinstance(op, TableScan)]

  @property
  def joins(self):
    return [op for (_, op) in self.flatten() if isinstance(op, Join)]

  # Get basic sources (TableScan + Unary Operators) -- similar to flatten()
  @property
  def sources(self):
    if self.root:
      result = []
      queue = deque([self.root])

      while queue:
        operator = queue.popleft()
        if operator.deep_max_arity <= 1:
          result.append(operator)
        else:
          queue.extendleft(operator.inputs())

      return result

  # Pre-order depth-first flattening of the query tree.
  def flatten(self):
    if self.root:
      result = []
      queue  = deque([(0, self.root)])

      while queue:
        (depth, operator) = queue.popleft()
        children = operator.inputs()
        result.append((depth, operator))
        if children:
          queue.extendleft([(depth+1, c) for c in children])

      return result


  # Plan preparation and execution

  # Returns a prepared plan, where every operator has filled in
  # internal parameters necessary for processing data.
  def prepare(self, database):
    if self.root:
      for (_, operator) in self.flatten():
        operator.prepare(database)
      return self
    else:
      raise ValueError("Invalid query plan")

  # Iterator abstraction for query processing.
  # Thus, we can use: "for page in plan: ..."
  def __iter__(self):
    return iter(self.root)

  # Plan and statistics information.

  # Returns a description for the entire query plan, based on the
  # description of each individual operator.
  def explain(self):
    if self.root:
      planDesc = []
      indent = ' ' * 2
      for (depth, operator) in self.flatten():
        planDesc.append(indent * depth + operator.explain())

      return '\n'.join(planDesc)

  # Returns the cost of the plan, either as an estimate or as an actual cost
  # based on the boolean 'estimated' parameter.
  #
  # Returns the cost of a plan based on a joint CPU and I/O metric.
  # This should use the cardinality estimation capabilities of the statistics
  # manager to determine the size of each intermediate result and based on this
  # cardinality, include the CPU and I/O cost of the operator processing those
  # intermediate results.
  #
  # For example on a simple chain: Table1 -> Operator1 -> Operator2 we want to
  # compute:
  # IO(Table1) + f * |Table1| * CPU(Operator1) + 2 * IO(|Operator1 outputs|)
  #            + f * |Operator1 outputs| * CPU(Operator2) + 2*IO(|Operator2 outputs|)
  #
  # where we pay a cost of 2 * IO costs for both operators 1 and 2 to write and read
  # temporary output files.
  #
  # Above, 'f' is a used-supplied scale factor indicating the importance of I/O
  # operations vs CPU operations in the cost model.
  #
  # Above the values of |Table1|, |Operator1 outputs|, and |Operator2 outputs| should
  # be determined using the statistics manager's cardinality estimation over subplans.
  #
  # For the actual cost, each operator should determine its own local cost added to the
  # cost of its children.
  def cost(self, estimated):
    return self.root.cost(estimated)

  # Sample-based statistics estimation, taking the desired sampling ratio as an argument.
  # This configures all operators in the plan to use sampling, and then runs the query plan.
  # Each operator tracks its estimated statistics during execution while in sampling mode.
  # We iterate over all tuples produced as the sampled query result, counting the result
  # cardinality. This cardinality is scaled up by the given factor to match the
  # original dataset from which the sample was taken, that is:
  #
  #     scaleFactor = actual dataset size / desired sample dataset size
  #
  def sample(self, scaleFactor):
    self.root.useSampling(True, scaleFactor)
    # Process query, update each operator's cost, cardinality, and selectivity estimates.
    for page in self:
      for tup in page[1]:
        self.sampleCardinality += 1

    # Leave the scale factor unchanged, so that we can correctly use estimated statistics after sampling.
    self.root.useSampling(False, scaleFactor)
    return self.sampleCardinality * scaleFactor

  def pushdownOperators(self):
    self.root = self.root.pushdownOperators()
    return self

class PlanBuilder:
  """
  A query plan builder class that can be used for LINQ-like construction of queries.

  A plan builder consists of an operator field, as the running root of the query tree.
  Each method returns a plan builder instance, that can be used to further
  operators compose with additional builder methods.

  A plan builder yields a Query.Plan instance through its finalize() method.

  >>> import Database
  >>> db = Database.Database()
  >>> db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  >>> schema = db.relationSchema('employee')

  # Populate relation
  >>> for tup in [schema.pack(schema.instantiate(i, 2*i+20)) for i in range(20)]:
  ...    _ = db.insertTuple(schema.name, tup)
  ...

  ### SELECT * FROM Employee WHERE age < 30
  >>> query1 = db.query().fromTable('employee').where("age < 30").finalize()

  >>> query1.relations()
  ['employee']

  >>> print(query1.explain()) # doctest: +ELLIPSIS
  Select[...,cost=...](predicate='age < 30')
    TableScan[...,cost=...](employee)

  >>> [schema.unpack(tup).age for page in db.processQuery(query1) for tup in page[1]]
  [20, 22, 24, 26, 28]


  ### SELECT eid FROM Employee WHERE age < 30
  >>> query2 = db.query().fromTable('employee').where("age < 30").select({'id': ('id', 'int')}).finalize()

  >>> print(query2.explain()) # doctest: +ELLIPSIS
  Project[...,cost=...](projections={'id': ('id', 'int')})
    Select[...,cost=...](predicate='age < 30')
      TableScan[...,cost=...](employee)

  >>> [query2.schema().unpack(tup).id for page in db.processQuery(query2) for tup in page[1]]
  [0, 1, 2, 3, 4]


  ### SELECT * FROM Employee UNION ALL Employee
  >>> query3 = db.query().fromTable('employee').union(db.query().fromTable('employee')).finalize()

  >>> print(query3.explain()) # doctest: +ELLIPSIS
  UnionAll[...,cost=...]
    TableScan[...,cost=...](employee)
    TableScan[...,cost=...](employee)

  >>> [query3.schema().unpack(tup).id for page in db.processQuery(query3) for tup in page[1]] # doctest:+ELLIPSIS
  [0, 1, 2, ..., 19, 0, 1, 2, ..., 19]



  """

  def __init__(self, **kwargs):
    other    = kwargs.get("other", None)
    operator = kwargs.get("operator", None)
    db       = kwargs.get("db", None)

    if other:
      self.fromOther(other)
    else:
      self.operator = operator
      self.database = db

    if self.operator is None and self.database is None:
      raise ValueError("No initial operator or database given for a plan builder")

  def fromOther(self, other):
    self.database = other.database
    self.operator = other.operator

  def fromTable(self, relId):
    if self.database:
      schema = self.database.relationSchema(relId)
      return PlanBuilder(operator=TableScan(relId, schema), db=self.database)

  def where(self, conditionExpr):
    if self.operator:
      return PlanBuilder(operator=Select(self.operator, conditionExpr), db=self.database)
    else:
      raise ValueError("Invalid where clause")

  def select(self, projectExprs):
    if self.operator:
      return PlanBuilder(operator=Project(self.operator, projectExprs), db=self.database)
    else:
      raise ValueError("Invalid select list")

  def join(self, rhsQuery, **kwargs):
    if rhsQuery:
      rhsPlan = rhsQuery.operator
    else:
      raise ValueError("Invalid Join RHS query")

    lhsPlan = self.operator
    return PlanBuilder(operator=Join(lhsPlan, rhsPlan, **kwargs), db=self.database)

  def union(self, subQuery):
    if self.operator:
      return PlanBuilder(operator=Union(self.operator, subQuery.operator), db=self.database)
    else:
      raise ValueError("Invalid union clause")

  def groupBy(self, **kwargs):
    if self.operator:
      return PlanBuilder(operator=GroupBy(self.operator, **kwargs), db=self.database)
    else:
      raise ValueError("Invalid group by operator")

  def order(self, **kwargs):
    if self.operator:
      return PlanBuilder(operator=Sort(self.operator, **kwargs), db=self.database)
    else:
      raise ValueError("Invalid order by operator")

  # Constructs a plan instance from the running plan tree.
  def finalize(self):
    if self.operator:
      plan = Plan(root=self.operator)
      if self.database:
        plan.prepare(self.database)
      return plan
    else:
      raise ValueError("Invalid query plan")


if __name__ == "__main__":
    import doctest
    doctest.testmod()
