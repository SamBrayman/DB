import itertools

from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from collections import OrderedDict
from Catalog.Schema        import DBSchema
import operator

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
    return plan.finalize().cost(True)

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
    operatorType = operator.operatorType()

    if operatorType == "TableScan":
      return operator
    elif operatorType == "GroupBy" or operatorType == "Sort":
      operator.subPlan = self.pushdownHelper(operator.subPlan)
      return operator
    elif operatorType == "UnionAll":
      operator.lhsPlan = self.pushdownHelper(operator.lhsPlan)
      operator.rhsPlan = self.pushdownHelper(operator.rhsPlan)
      return operator
    elif operatorType[-4:] == "Join":
      operator.lhsPlan = self.pushdownHelper(operator.lhsPlan)
      operator.rhsPlan = self.pushdownHelper(operator.rhsPlan)
      return operator

    #first check if valid operatorType
    #if operatorType != "Project" and operatorType != "Select" and operatorType != "GroupBy" and operatorType != "Sort" and operatorType != "UnionAll" and operatorType[-4:] != "Join":
      #return operator

    elif operatorType == "Project":
      operator.subPlan = self.pushdownHelper(operator.subPlan)
      subplanType = operator.subPlan.operatorType()

      if subplanType == "GroupBy" or subplanType == "TableScan":
        return operator

      #call second helper
      if subplanType == "Select":

        '''
        Check keys - if not in keys, cannot pushdown anymore
        '''
        outAtrributes = set(operator.projectExprs.keys())
        att = ExpressionInfo(operator.subPlan.selectExpr).getAttributes()
        keys = operator.projectExprs.keys()
        setKeys = set(keys)
        op = operator
        if att.issubset(setKeys):
          op = operator
          op = operator.subPlan
          operator.subPlan = operator.subPlan.subPlan
          op.subPlan = self.pushdownHelper(operator)

        #if select not in keys:
            #return operator

        #operator.subPlan = operator.subPlan.subPlan
        #operator.subPlan.subPlan = self.pushdownHelper(operator)
        return op


      #redundancy
      elif subplanType == "Project":
        test = [op.subPlan.projectExprs[key][0].isAttribute() for key in op.projectExprs]
        if False not in test:
          operator.subPlan = operator.subPlan.subPlan
        return self.pushdownOperator(operator)



      elif subplanType[-4:] == "Join":

        #get keys and values
        #items = operator.projectExprs.items()

        right = operator.subPlan.rhsPlan.schema().fields
        rightSet = set(right)
        rightProject = {}

        left = operator.subPlan.lhsPlan.schema().fields
        leftSet = set(left)
        leftProject = {}

        anythingRemaining = False
        #for (attribute, (expr, rand)) in items:
        for e in operator.projectExprs:
          pros = ExpressionInfo(operator.projectExprs[e][0]).getAttributes()
          if pros.issubset(leftSet):
            leftProject[e] = operator.projectExprs[e]
          if pros.issubset(rightSet):
            rightProject[e] = operator.projectExprs[e]
          else:
            anythingRemaining = True

          #if leftProject:
            #operator.subPlan.lhsPlan = self.pushdownHelper

          '''
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
        '''

        #if left dictionary not empty
        #remember empty dic evaluates to false
        if leftProject:
          lPlan = operator.subPlan.lhsPlan
          operator.subPlan.lhsPlan = self.pushdownHelper(Project(lPlan, leftProject))

        if rightProject:
          rPlan = operator.subPlan.rhsPlan
          operator.subPlan.rhsPlan = self.pushdownHelper(Project(rPlan, rightProject))


        #length check - must be same size iIOT pushdown
        #lSize = len(operator.projectExprs)
        #rightSize = len(rightProject)


        #if fullSize != (rightSize + leftSize):
          #return operator

        op = operator
        if not anythingRemaining:
          if leftProject and rightProject:
            op = operator.subPlan
          return op

      #end subPlan "Join"

      elif subplanType == "Sort":
        return operator

      elif subplanType == "UnionAll":
        #subP = operator.subPlan
        #subP
        tempLeft = Project(operator.subPlan.lhsPlan)
        tempRight = Project(operator.subPlan.rhsPlan)

        operator.subPlan.lhsPlan = self.pushdownHelper(tempLeft, operator.projectExprs)
        operator.subPlan.rhsPlan = self.pushdownHelper(tempRight, operator.projectExprs)
        return operator.subPlan

      #else not Join or Union
      else:
        #return operator
        #erro if this happens
        print(subplanType)
        raise NotImplementedError

      #return operator.subPlan

    #end "Project"

    #safety check above, so operatorType must be "Select"
    elif operatorType == "Select":

      #first part same as with "Project": subPlan pushdown
      operator.subPlan = self.pushdownHelper(operator.subPlan)

      subplanType = operator.subPlan.operatorType()

      if subplanType == "GroupBy" or subplanType == "TableScan" or subplanType == "Project":
        return operator

      if subplanType == "Select":
        op = operator
        op = operator.subPlan
        operator.subPlan = operator.subPlan.subPlan
        operator.subPlan.subPlan = self.pushdownHelper(operator)
        return operator.subPlan

      if subplanType == "Sort":
        operator.subPlan = operator.subPlan.subPlan
        operator.subPlan.subPlan = self.pushdownHelper(operator)
        return operator.subPlan

      elif subplanType[-4:] == "Join":

        selectExpress = ExpressionInfo(operator.selectExpr).decomposeCNF()



        left = operator.subPlan.lhsPlan.schema().fields
        right = operator.subPlan.rhsPlan.schema().fields
        leftSet = set(operator.subPlan.lhsPlan.schema().fields)
        rightSet = set(operator.subPlan.rhsPlan.schema().fields)


        leftExpress = []
        rightExpress = []
        unpushedExpress = []



        for expr in selectExpress:
          select = ExpressionInfo(expr).getAttributes()
          if select.issubset(leftSet):
            leftExpress.append(expr)
          elif select.issubset(rightSet):
            rightExpress.append(expr)
          else:
            unpushedExpress.append(expr)


        if leftExpress:
          newLeft = ' and '.join(leftExpress)
          #lSelect
          operator.subPlan.lhsPlan = self.pushdownHelper(Select(operator.subPlan.lhsPlan, newLeft))

        if rightExpress:
          newRight = ' and '.join(rightExpress)
          op.subPlan.rhsPlan = self.pushdownHelper(Select(operator.subPlan.rhsPlan, newRight))

        if unpushedExpress:
          return Select(operator.subPlan, ' and '.join(unpushedExpress))
        else:
          return operator.subPlan

    elif operatorType == "UnionAll":
      operator.subPlan.lhsPlan = self.pushdownHelper(Select(operator.subPlan.lhsPlan, operator.selectExpr))
      operator.subPlan.rhsPlan = self.pushdownHelper(Select(operator.subPlan.rhsPlan, operator.selectExpr))
      return operator.subPlan

    elif operatorType == "GroupBy" or operatorType == "Sort":
      operator.subPlan = self.pushdownHelper(operator.subPlan)
      return operator

    else:
      raise NotImplementedError

    #else:
      #return operator



  def getOrigPlan(self, plan,ops,count):
    '''
    print(ops)
    if(plan.root.operatorType() != "TableScan" and "Join" not in plan.root.operatorType() and plan.root.subPlan):
        optype = plan.root.operatorType()
        print(optype)
        if(optype == "Project"):
            ops[optype + str(count)] = plan.root.projectExprs
            count += 1
            ops = self.getOrigPlan(plan.root.subPlan,ops,count)
        elif(optype == "Select"):
            ops[optype + str(count)] = plan.root.selectExpr
            count += 1
            ops = self.getOrigPlan(plan.root.subPlan,ops,count)
        elif(optype == "GroupBy"):
            ops[optype + str(count)] =(plan.root.groupSchema,plan.root.aggSchema,plan.root.groupExpr,plan.root.aggExprs \
            ,plan.root.groupHashFn)
            count += 1
            ops = self.getOrigPlan(plan.root.subPlan,ops,count)

        elif(optype == "TableScan"):
            ops[optype + str(count)] = (plan.root().relId, plan.root().schema)
            count += 1

    '''
    '''
    string = plan.explain()
    for line in string:
        optype = line[0:line.index("[")]
        if(optype == "Project"):
            ops[optype + str(count)] = line[line.index("{"):line.index("}")]
        if(optype == "Select"):
            ops[optype + str(count)] = line[line.index("\'"):line[index("\'") + 1:].index("\'")]
        if(optype == "GroupBy"):
            atts = line[line.index("[") + 1:]
            groupSchema = atts[atts.index("=") + 1 : atts.index(", aggschema")]
            atts = atts[atts.index(", aggschema"):]
    '''
    for elem in plan.flatten():
        optype = elem[1].operatorType()
        #print(optype)
        if(optype == "Project"):
            ops[optype + str(count)] = elem[1].projectExprs
            count += 1
        elif(optype == "Select"):
            ops[optype + str(count)] = elem[1].selectExpr
            count += 1
        elif(optype == "GroupBy"):
            ops[optype + str(count)] =(elem[1].groupSchema,elem[1].aggSchema,elem[1].groupExpr,elem[1].aggExprs \
            ,elem[1].groupHashFn)
            count += 1

    return ops

  # Returns an optimized query plan with joins ordered via a System-R style
  # dyanmic programming algorithm. The plan cost should be compared with the
  # use of the cost model below.
  def pickJoinOrder(self, plan):
    #raise NotImplementedError
    relations = plan.relations()
    expressions = {}
    for relation in relations:
        for elem in self.db.relationSchema(relation).fields:
            if( 'KEY' in elem):
                if(relation in expressions):
                    #print(expressions)
                    #print(relation)
                    #print(elem)
                    expressions[relation].append(elem)
                    #print(expressions)
                    #print(relation)
                    #print(elem)
                else:
                    #print(expressions)
                    #print(relation)
                    #print(elem)
                    expressions[relation] = [elem]
                    #print(expressions)
                    #print(relation)
                    #print(elem)
    n = len(relations)
    if(n == 1):
        return plan
    optplan = None
    for i in range(n-1):
        if(i == 0 ):
            for j in range(n):
                for k in range(n):
                    if(k != j):
                        #We get costs for pairs and keep the plan object for each pair
                        #For triple we just use plan and make new join and recalc cost
                        expr1 = ""
                        expr2 = ""
                        #Find if there is a possible join expression between the two relations
                        for elem in expressions[relations[j]]:
                            #print(elem)
                            #print([expr[2:] for expr in expressions[relations[k]]])
                            if(elem[2:] in [expr[2:] for expr in expressions[relations[k]]]):
                                expr1 = elem
                                expr2 = expressions[relations[k]][0][0:2] + elem[2:]
                        #print(expr1)
                        #print(expr2)
                        if(expr1 != "" and expr2 != ""):
                            tempplan = self.db.query().fromTable(relations[j]).join(self.db.query().fromTable(relations[k]),\
                            method ='block-nested-loops', expr = expr1 + " == " + expr2)
                            cost = self.getPlanCost(tempplan)
                            self.addPlanCost(tempplan,cost)
                            keySchema = DBSchema('key1', [(expr1,'int')])
                            keySchema1 = DBSchema('key2', [(expr2,'int')])
                            tempplan = self.db.query().fromTable(relations[j]).join(self.db.query().fromTable(relations[k]),\
                            method ='hash', expr = expr1 + " == " + expr2, rhsSchema = self.db.relationSchema(relations[k]),\
                            lhsHashFn='hash(' + expr1 + ') % 11', lhsKeySchema = keySchema, \
                            rhsHashFn='hash(' + expr2 + ') % 11', rhsKeySchema = keySchema1)
                            cost = self.getPlanCost(tempplan)
                            self.addPlanCost(tempplan,cost)
            optplan = max(self.statsCache.items(), key=operator.itemgetter(1))[0]
            #print(optplan)
            self.statsCache = {}
            j=0 #Reset j for next loop
        if(i != 0 and n != 2):
            currrelations = optplan.finalize().relations()
            elements = []
            for relation in currrelations:
                for expr in expressions[relation]:
                    elements.append(expr)
            for j in range(n):
#                for k in range(n):
                    #We get costs for pairs and keep the plan object for each pair
                    #For triple we just use plan and make new join and recalc cost
                    expr1 = ""
                    expr2 = ""
                    #Find if there is a possible join expression between the two relations
                    #print([relation for relation in currrelations])
                    #print(expressions[relations[j]])
#                    for relation in optplan.finalize().relations():
                    if(relations[j] not in currrelations):
                        for elem in elements:
                            #print(elem[2:])
                            #print([expr[2:] for expr in expressions[relations[j]]])
                            if(elem[2:] in [expr[2:] for expr in expressions[relations[j]]] and elem not in [expr for expr in expressions[relations[j]]]):
                                expr1 = elem
                                expr2 = expressions[relations[j]][0][0:2] + elem[2:]
                            #print(expr1)
                            #print(expr2)
                            if(expr1 != "" and expr2 != ""):
                                tempplan = optplan.join(self.db.query().fromTable(relations[j]),\
                                method ='block-nested-loops', expr = expr1 + " == " + expr2)
                                cost = self.getPlanCost(tempplan)
                                self.addPlanCost(tempplan,cost)
                                #print(self.statsCache)
                                keySchema = DBSchema('key1', [(expr1,'int')])
                                keySchema1 = DBSchema('key2', [(expr2,'int')])
                                tempplan = optplan.join(self.db.query().fromTable(relations[j]),\
                                method ='hash', expr = expr1 + " == " + expr2, \
                                lhsHashFn='hash(' + expr1 + ') % 11', lhsKeySchema = keySchema,rhsSchema = self.db.relationSchema(relations[j]), \
                                rhsHashFn='hash(' + expr2 + ') % 11', rhsKeySchema = keySchema1)
                                cost = self.getPlanCost(tempplan)
                                self.addPlanCost(tempplan,cost)
                                #print(self.statsCache)
                                expr1 = ""
                                expr2 = ""
            optplan = max(self.statsCache.items(), key=operator.itemgetter(1))[0]
            #print(optplan)
            self.statsCache = {}
            j=0
    #print(optplan)
    ops = OrderedDict()
    count =0
    if(optplan != None):
        operators = self.getOrigPlan(plan,ops,count)
        #print(reversed(list(operators.items())))
        for (key,value) in reversed(list(operators.items())):
            if("Project" in key):
                optplan = optplan.select(value)
                #print(optplan)
            elif("GroupBy" in key):
                optplan = optplan.groupBy(groupSchema=value[0],aggSchema=value[1],groupExpr=value[2],aggExprs=value[3],groupHashFn=value[4])
            elif("Select" in key):
                optplan = optplan.where(value)
                #print(optplan)
        #'''
        #elif("TableScane" in key):
        #    optplan.from()
        #'''
        return optplan.finalize()




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




  # Optimize the given query plan, returning the resulting improved plan.
  # This should perform operation pushdown, followed by join order selection.
  def optimizeQuery(self, plan):
    pushedDown_plan = self.pushdownOperators(plan)
    joinPicked_plan = self.pickJoinOrder(pushedDown_plan)

    return joinPicked_plan

if __name__ == "__main__":
  import doctest
  doctest.testmod()

