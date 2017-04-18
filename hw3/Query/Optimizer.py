import itertools

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from Collections import OrderedDict

class Optimizer:
  """
  A query optimization class.

  This implements System-R style query optimization, using dynamic programming.
  We only consider left-deep plan trees here.

  We provide doctests for example usage only.
  Implementations and cost heuristics may vary.

  >>> import Database
  >>> db = Database.Database()
  >>> try:
  ...   db.createRelation('department', [('did', 'int'), ('eid', 'int')])
  ...   db.createRelation('employee', [('id', 'int'), ('age', 'int')])
  ... except ValueError:
  ...   pass

  # Join Order Optimization
  >>> query4 = db.query().fromTable('employee').join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid').finalize()

  >>> db.optimizer.pickJoinOrder(query4)

  # Pushdown Optimization
  >>> query5 = db.query().fromTable('employee').union(db.query().fromTable('employee')).join( \
        db.query().fromTable('department'), \
        method='block-nested-loops', expr='id == eid')\
        .where('eid > 0 and id > 0 and (eid == 5 or id == 6)')\
        .select({'id': ('id', 'int'), 'eid':('eid','int')}).finalize()

  >>> db.optimizer.pushdownOperators(query5)

  """

  def __init__(self, db):
    self.db = db
    self.statsCache = {}

  # Caches the cost of a plan computed during query optimization.
  def addPlanCost(self, plan, cost):
    raise NotImplementedError

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    raise NotImplementedError

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    return Plan(root=self.pushdownOperator(plan.root))

  def pushdownOperator(self, op):
    if op.operatorType() == "TableScan":
      return op
    elif op.operatorType() in ["GroupBy", "Sort"]:
      op.subPlan = self.pushdownOperator(op.subPlan)
      return op
    elif op.operatorType() == "UnionAll" or "Join" in op.operatorType():
      op.lhsPlan = self.pushdownOperator(op.lhsPlan)
      op.rhsPlan = self.pushdownOperator(op.rhsPlan)
      return op
    elif op.operatorType() == "Project":
      return self.pushdownProject(op)
    elif op.operatorType() == "Select":
      return self.pushdownSelect(op)
    else:
      print("Unmatched operatorType in pushdownOperator(): " + op.operatorType())
      raise NotImplementedError

  def pushdownProject(self, op):
    # First pushdown operators below:
    op.subPlan = self.pushdownOperator(op.subPlan)

    if op.subPlan.operatorType() in ["GroupBy", "TableScan"]:
      return op

    elif op.subPlan.operatorType() == "Project":
      # Attempt to remove redundant projections:
      bools = [op.subPlan.projectExprs[key][0].isAttribute() for key in op.projectExprs]
      if False not in bools:
        op.subPlan = op.subPlan.subPlan
      return self.pushdownOperator(op)

    elif op.subPlan.operatorType() == "Select":
      # Move op below its subplan if op provides all attributes needed for the selectExpr
      selectAttrs = ExpressionInfo(op.subPlan.selectExpr).getAttributes()
      outputAttrs = set(op.projectExprs.keys())
      result = op
      if selectAttrs.issubset(outputAttrs):
        result = op.subPlan
        op.subPlan = result.subPlan
        result.subPlan = self.pushdownOperator(op)
      return result

    elif op.subPlan.operatorType() == "Sort":
      # TODO
      return op

    elif op.subPlan.operatorType() == "UnionAll":
      # Place a copy of op on each side of the union
      result = op.subPlan
      result.lhsPlan = self.pushdownOperator(Project(result.lhsPlan, op.projectExprs))
      result.rhsPlan = self.pushdownOperator(Project(result.rhsPlan, op.projectExprs))
      return result

    elif "Join" in op.subPlan.operatorType():
      # Partition the projections among the input relations, as much as possible
      lhsAttrs = set(op.subPlan.lhsPlan.schema().fields)
      rhsAttrs = set(op.subPlan.rhsPlan.schema().fields)
      lhsProjectExprs = {}
      rhsProjectExprs = {}
      remainingProjectExprs = False

      for attr in op.projectExprs:
        requiredAttrs = ExpressionInfo(op.projectExprs[attr][0]).getAttributes()
        if requiredAttrs.issubset(lhsAttrs):
          lhsProjectExprs[attr] = op.projectExprs[attr]
        elif requiredAttrs.issubset(rhsAttrs):
          rhsProjectExprs[attr] = op.projectExprs[attr]
        else:
          remainingProjectExprs = True

      if lhsProjectExprs:
        op.subPlan.lhsPlan = self.pushdownOperator(Project(op.subPlan.lhsPlan, lhsProjectExprs))
      if rhsProjectExprs:
        op.subPlan.rhsPlan = self.pushdownOperator(Project(op.subPlan.rhsPlan, rhsProjectExprs))

      result = op
      # Remove op from the tree if there are no remaining project expressions, and each side of the join recieved a projection
      if not remainingProjectExprs and lhsProjectExprs and rhsProjectExprs:
        result = op.subPlan
      return result
    else:
      print("Unmatched operatorType in pushdownOperator(): " + op.operatorType())
      raise NotImplementedError

  def pushdownSelect(self, op):
    # First pushdown operators below:
    op.subPlan = self.pushdownOperator(op.subPlan)

    if op.subPlan.operatorType() in ["GroupBy", "TableScan", "Project"]:
      return op

    elif op.subPlan.operatorType() == "Select":
      # Reorder two selects based on 'score'
      useEstimated = True
      opScore = (1 - op.selectivity(useEstimated)) / op.tupleCost
      childScore = (1 - op.subPlan.selectivity(useEstimated)) / op.tupleCost

      result = op
      if childScore > myScore:
        result = op.subPlan
        op.subPlan = result.subPlan
        result.subPlan = self.pushdownOperator(op)
      return result

    elif op.subPlan.operatorType() == "Sort":
      # Always move a select below a sort
      result = op.subPlan
      op.subPlan = result.subPlan
      result.subPlan = self.pushdownOperator(op)
      return result

    elif op.subPlan.operatorType() == "UnionAll":
      # Place a copy of op on each side of the union
      result = op.subPlan
      result.lhsPlan = self.pushdownOperator(Select(result.lhsPlan, op.selectExpr))
      result.rhsPlan = self.pushdownOperator(Select(result.rhsPlan, op.selectExpr))
      return result

    elif "Join" in op.subPlan.operatorType():
      # Partition the select expr as much as possible
      exprs = ExpressionInfo(op.selectExpr).decomposeCNF()
      lhsExprs = []
      rhsExprs = []
      remainingExprs = []

      lhsAttrs = set(op.subPlan.lhsPlan.schema().fields)
      rhsAttrs = set(op.subPlan.rhsPlan.schema().fields)

      for e in exprs:
        attrs = ExpressionInfo(e).getAttributes()
        if attrs.issubset(lhsAttrs):
          lhsExprs.append(e)
        elif attrs.issubset(rhsAttrs):
          rhsExprs.append(e)
        else:
          remainingExprs.append(e)

      if lhsExprs:
        newLhsExpr = ' and '.join(lhsExprs)
        lhsSelect = Select(op.subPlan.lhsPlan, newLhsExpr)
        op.subPlan.lhsPlan = self.pushdownOperator(lhsSelect)

      if rhsExprs:
        newRhsExpr = ' and '.join(rhsExprs)
        rhsSelect = Select(op.subPlan.rhsPlan, newRhsExpr)
        op.subPlan.rhsPlan = self.pushdownOperator(rhsSelect)

      result = None
      if remainingExprs:
        newExpr = ' and '.join(remainingExprs)
        result = Select(op.subPlan, newExpr)
      else:
        result = op.subPlan

      return result
    else:
      print("Unmatched operatorType in pushdownOperator(): " + op.operatorType())
      raise NotImplementedError

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    #raise NotImplementedError
    relations = plan.relations()
    expressions = {}
    for relation in relations:
        for elem in self.db.relationSchema(relation):
            if( 'KEY' in elem[0]):
                if(relation in expression):
                    expressions[relation] = expressions[relation].append(elem[0])
                else:
                    expressions[relation] = [elem[0]]
    n = len(relations)
    for i in range(n):
        if(i != 0 and i != 1 and i==2):
            for j in range(n):
                for k in range(n):
                    #We get costs for pairs and keep the plan object for each pair
                    #For triple we just use plan and make new join and recalc cost
                    expr1 = ""
                    expr2 = ""
                    for elem in expressions[relations[i]]:
                        if(elem[2:] in [expr[2:] for expr in expressions[relations[k]]]):
                            expr1 = elem
                            expr2 = expressions[relations[k]][0][0:2] + elem[2:]
                            break
                    if(expr1 != "" and expr2 != ""):
                        tempplan = self.db.query().fromTable(relations[j]).join(db.query().fromTable(relations[k]),\
                        method ='block-nested-loops', expr = expr1 + " == " + expr2)
                        cost = self.getPlanCost(tempplan)
                        self.addPlanCost(tempplan,cost)
                        keySchema = DBSchema('key1', [(expr1,'int')])
                        keySchema1 = DBSchema('key2', [(expr2,'int')])
                        tempplan = self.db.query().fromTable(relations[j]).join(db.query().fromTable(relations[k]),\
                        method ='hash', expr = expr1 + " == " + expr2, \
                        lshHashFn='hash(' + expr1 + ') % 11', lhsKeySchema = keySchema, \
                        rhsHashFn='hash(' + expr2 + ') % 11', rhsKeySchema = keySchema2)
                        cost = self.getPlanCost(tempplan)
                        self.addPlanCost(tempplan,cost)
            optplan = max(self.statsCache.iteritems(), key=operator.itemgetter(1))[0]
            self.statsCache = {}
            j=0
        if(i != 0 and i != 1 and i!=2):
            for j in range(n):
#                for k in range(n):
                    #We get costs for pairs and keep the plan object for each pair
                    #For triple we just use plan and make new join and recalc cost
                    expr1 = ""
                    expr2 = ""
                    for elem in expressions[relations[i]]:
                        if(elem[2:] in [expr[2:] for expr in expressions[relations[k]]]):
                            expr1 = elem
                            expr2 = expressions[relations[k]][0][0:2] + elem[2:]
                            break
                    tempplan = optplan.join(db.query().fromTable(relations[j]),\
                    method ='block-nested-loops', expr = expr1 + " == " + expr2)
                    cost = self.getPlanCost(tempplan)
                    self.addPlanCost(tempplan,cost)
                    keySchema = DBSchema('key1', [(expr1,'int')])
                    keySchema1 = DBSchema('key2', [(expr2,'int')])
                    tempplan = optplan.join(db.query().fromTable(relations[j]),\
                    method ='hash', expr = expr1 + " == " + expr2, \
                    lhsHashFn='hash(' + expr1 + ') % 11', lhsKeySchema = keySchema, \
                    rhsHashFn='hash(' + expr2 + ') % 11', rhsKeySchema = keySchema2)
                    cost = self.getPlanCost(tempplan)
                    self.addPlanCost(tempplan,cost)
            optplan = max(self.statsCache.iteritems(), key=operator.itemgetter(1))[0]
            self.statsCache = {}
            j=0
    operators = self.getOrigPlan(optplan)
    for (key,value) in operators.items():
        if("Project" in key):
            optplan.select(value)
        elif("GrouBy" in key):
            optplan.groupBy(groupSchema=value[0],aggSchema=value[1],groupExpr=value[2],aggExprs=value[3],groupHashFn=value[4])
        elif("Select" in key):
            optplan.where(value)
        '''
        elif("TableScane" in key):
            optplan.from()
        '''
    return optplan


    '''
    if(plan.root().subPlan):
        plan.root().subPlan = pickJoinOrder(plan.root().subPlan)
        if("Join" in plan.root().operatorType()):

        else:
            return plan.root()
    else:
        if("Join" in plan.root().operatorType()):

        else:
            return plan.root()
   '''

  count = 0
  ops = OrderedDict()
  def getOrigPlan(self, plan,ops):
    if(plan.subplan):
        optype = plan.root().operatorType()
        if(optype == "Project"):
            ops[optype + str(count)] = plan.root().projectExprs
            count += 1
        elif(optype == "Select"):
            ops[optype + str(count)] = plan.root().selectExpr
            count += 1
        elif(optype == "GroupBy"):
            ops[optype + str(count)] =(plan.root().groupSchema,plan.root().aggSchema,plan.root().groupExpr,plan.root().aggExprs \
            ,plan.root().groupHashFn)
            count += 1
        '''
        elif(optype == "TableScan"):
            ops[optype + str(count)] = (plan.root().relId, plan.root().schema)
            count += 1
        '''
        ops = getOrigPlan(plan.subplan,ops)
    return ops


  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()

