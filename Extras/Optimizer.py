import itertools
import operator
from collections import OrderedDict
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo

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
    self.statsCache[plan] = cost

  # Checks if we have already computed the cost of this plan.
  def getPlanCost(self, plan):
    return plan.cost(true)

  # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    # Given a plan, return an optimized plan with both selection and
  # projection operations pushed down to their nearest defining relation
  # This does not need to cascade operators, but should determine a
  # suitable ordering for selection predicates based on the cost model below.
  def pushdownOperators(self, plan):
    #raise NotImplementedError
    if plan:
      planRoot = self.pushdownHelper(plan.root)
      return Plan(root=planRoot)


  def pushdownHelper(self, operator):
    #first determine operator type
    opertorType = operator.operatorType()

    #first check if valid operatorType
    if operatorType != "Project" and operatorType != "Select" and operatorType != "GroupBy" and operatorType != "Sort" and operatorType != "UnionAll" and operatorType[-4:] != "Join":
      return operator

    elif operatorType == "Project":
      operator.subPlan = self.pushdownHelper(operator.subPlan)
      subplanType = operator.subPlan.operatorType()

      #call second helper
      if subplanType == "Select":

        '''
        Check keys - if not in keys, cannot pushdown anymore
        '''
        for select in ExpressionInfo(operator.subPlan.selectExpr).getAttributes():
          keys = operator.projectExprs.keys()
          if select not in keys:
            return operator

        operator.subPlan = operator.subPlan.subPlan
        operator.subPlan.subPlan = self.pushdownHelper(operator)

      elif subplanType[-4:] == "Join":

        items = operator.projectExprs.items()

        right = operator.subPlan.rhsPlan.schema().fields
        rightProject = {}

        left = operator.subPlan.lhsPlan.schema().fields
        leftProject = {}

        for (attribute, (expr, rand)) in items:
          pros = ExpressionInfo(expr)getAttributes()

          result = True
          #left
          for e in pros:
            if e not in left:
              result = False

          # if True
          if result:
            leftProject[attribute] = operator.projectExprs[attribute]
            continue

          #repeat with right now
          result = True
          for e in pros:
            if e not in right:
              result = False

          if result:
            rightProject[attribute] = operator.projectExprs[attribute]

        #end for

        #if left dictionary not empty
        #remember empty dic evaluates to false
        if leftProject:
          lPlan = operator.subPlan.lhsPlan
          operator.subPlan.lhsPlan = self.pushdownHelper(Project(lPlan, leftProject))

        if rightProject:
          rPlan = operator.subPlan.rhsPlan
          operator.subPlan.rhsPlan = self.pushdownHelper(Project(rPlan, rightProject))


        #length check - must be same size iIOT pushdown
        fullSize = len(operator.projectExprs)
        rightSize = len(rightProject)
        leftSize = len(leftProject)

        if fullSize != (rightSize + leftSize):
          return operator

      #end subPlan "Join"

      elif subplanType == "UnionAll":
        tempLeft = Project(operator.subPlan.lhsPlan)
        tempRight = Project(operator.subPlan.rhsPlan)

        operator.subPlan.lhsPlan = self.pushdownHelper(tempLeft, operator.projectExprs)
        operator.subPlan.rhsPlan = self.pushdownHelper(tempRight, operator.projectExprs)

      #else not Join or Union
      else:
        return operator

      return operator.subPlan

    #end "Project"

    #safety check above, so operatorType must be "Select"
    elif operatorType == "Select":

      #first part same as with "Project": subPlan pushdown
      operator.subPlan = self.pushdownHelper(operator.subPlan)
      subplanType = operator.subPlan.operatorType()

      if subplanType == "Sort" or "sort":
        operator.subPlan = operator.subPlan.subPlan
        operator.subPlan.subPlan = self.pushdownHelper(operator)
      elif subplanType[-4:] == "Join":

        selectExpress = ExpressionInfo(operator.selectExpr).decomposeCNF()



        left = operator.subPlan.lhsPlan.schema().fields
        right = operator.subPlan.rhsPlan.schema().fields
        leftExpress = []
        leftAttributes = set(operator.subPlan.lhsPlan.schema().fields)
        rightAttributes = set(operator.subPlan.rhsPlan.schema().fields)
        rightExpress = []
        unpushedExpress = []

        for expr in selectExpress:
          select = ExpressionInfo(selectExpr).getAttributes()
          if select.issubset(leftAttributes):
            left.append(select)
          elif select.issubset(rightAttributes):
            right.append(select)
          else:
            unpushedExpress.append(select)


        if leftExpress:
          newExpression = ' and '.join(leftExpress)
          #lSelect
          op.subPlan.lhsPlan = self.pushdownHelper(Select(operator.subPlan.lhsPlan, newExpression))

        if rightExpress:
          newExpression = ' and '.join(rightExpress)
          op.subPlan.rhsPlan = self.pushdownHelper(Select(operator.subPlan.rhsPlan, newExpression))

        if unpushedExpress:
          return Select(operator.subPlan, ' and '.join(unpushedExpress))

        else:
          return operator
        return operator.subPlan

    elif operatorType == "UnionAll" or operatorType[-4:] == "Join":
      operator.lhsPlan = self.pushdownHelper(operator.lhsPlan)
      operator.rhsPlan = self.pushdownHelper(operator.rhsPlan)
      return operator

    elif operatorType == "GroupBy" or operatorType == "Sort":
      operator.subPlan = self.pushdownHelper(operator.subPlan)
      return operator

    #else:
#return operator

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
