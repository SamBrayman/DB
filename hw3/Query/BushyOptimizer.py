import itertools

from Query.Optimizer import Optimizer
from Query.Plan import Plan
from Query.Operators.Join import Join
from Query.Operators.Project import Project
from Query.Operators.Select import Select
from Utils.ExpressionInfo import ExpressionInfo
from collections import OrderedDict
from Catalog.Schema        import DBSchema
import operator
from Query.Operators.TableScan import TableScan


class BushyOptimizer(Optimizer):

  def getPlanCost(self, plan):
    #return plan.finalize().cost(True)
    left = plan.root.lhsPlan
    right = plan.root.rhsPlan
    joinMethod = plan.root.joinMethod
    joinExpr = plan.root.joinExpr
    tup = (left, right, joinMethod, joinExpr)
    if tup in self.statsCache:
        return self.statsCache[tup]
    return None

  def pickJoinOrder(self, plan):

    numTables = 2
    typesOfJoins = ["nested-loops", "block-nested-loops"]
    self.count = 0
    tableId = []
    joinOperators = []
    #operators = {}
    #test = {}
    plans = {}
    fields = {}


    joinOp = None
    for (rand, operator) in plan.flatten():
        #joinOp = None
        if joinOp is None and not isinstance(operator, TableScan) and not isinstance(operator, Join) and isinstance(operator.subPlan, Join):
            joinOp = operator

        #now not none
        #check operator
        if isinstance(operator, Project):
            if isinstance(operator.subPlan, TableScan):
                currID = operator.subPlan.id()
                tableId.append(currID)

                plans[str(currID)] = operator
                fields[str(currID)] = operator.subPlan.schema().fields

        elif isinstance(operator, Select):
            if isinstance(operator.subPlan, TableScan):
                currID = operator.subPlan.id()
                tableId.append(currID)
                plans[str(currID)] = operator
                fields[currID] = op.subPlan.schema().fields

        elif isinstance(operator, Join):
            joinOperators.append(operator)

        elif isinstance(operator, TableScan):
            currIDStr = str(operator.id())
            if currIDStr not in plans:
                currID = operator.id()
                tableId.append(currID)
                plans[str(currID)] = operator
                fields[currID] = operator.schema().fields


    while numTables <= len(tableId):
        #need all combinations, useful itertools functionality
        #need all possible how to iterate through???
        combos = itertools.combinations(tableId, numTables)
        #pick optimal
        #minimum = None
        #selectedPlan = None
        for currOrder in combos:
            minimum = None
            selectedPlan = None
            '''
            now need to distribute for the bushy
            '''
            #why isn't range working???
            #add one, excludes last DUH
            for i in range(1, int(numTables/2) + 1):
                innerCombos = itertools.combinations(currOrder, i)

                for j in innerCombos:
                    leftId = list(j)
                    rightId = list(set(currOrder) - set(j))

                    leftKey = ','.join(str(ID) for ID in leftId)
                    rightKey = ','.join(str(ID) for ID in rightId)

                    leftOp = None
                    rightOp = None
                    if leftKey in plans:
                        leftOp = plans[leftKey]
                    if rightKey in plans:
                        rightOp = plans[rightKey]

                    '''
                    what to do if either not found...
                    if either leftOp or rightOp not found in plans dictionary
                    this iteration is not possible, skip to next iteration
                    '''
                    if leftOp is None or rightOp is None:
                        continue

                    #at this point leftOp and rightOp both relevant joins

                    attributes = []
                    #add left
                    for currID in leftId:
                        attributes.extend(fields[currID])
                    #add right
                    for currID in rightId:
                        attributes.extend(fields[currID])

                    currExpr = None

                    for join in joinOperators:
                        expr = join.joinExpr
                        if expr:
                            currAttributes = ExpressionInfo(join.joinExpr).getAttributes()
                            currSet = set(currAttributes)
                            attSet = set(attributes)
                            if currSet.issubset(attSet):
                                currExpr = expr
                                break

                    '''
                    if currExpr is none after loop, skip
                    '''
                    if currExpr is None:
                        continue;

                    self.count = self.count + 2

                    '''
                    now test all different types of joins
                    IOT compare their costs,
                    choose min cost
                    '''
                    for currMethod in typesOfJoins:
                        #first try left as left, right as right
                        #will switch in next loop
                        j = Join(lhsPlan=leftOp, rhsPlan=rightOp, method=currMethod, expr=currExpr)
                        testPlan = Plan(root=j)

                        testPlan.prepare(self.db)
                        
                        #Sampling involves a lot of overhead....
                        testPlan.sample(1.0)

                        currCost = self.getPlanCost(plan)
                        if currCost is None:
                            currCost = testPlan.cost(estimated=True)

                        self.addPlanCost(plan, currCost)


                        #now see if currCost better than minimum
                        #if so, update selectPlan and minimum cost
                        if minimum is None or (minimum > currCost):
                            selectedPlan = testPlan
                            minimum = currCost


                    #now try switching LHS and RHS to see if better cost
                    for currMethod in typesOfJoins:
                        #first try left as left, right as right
                        #will switch in next loop
                        j = Join(lhsPlan=rightOp, rhsPlan=leftOp, method=currMethod, expr=currExpr)
                        testPlan = Plan(root=j)

                        testPlan.prepare(self.db)
                        #Sampling involves way too much overhead....

                        #testPlan.sample(1.0)

                        currCost = self.getPlanCost(plan)
                        if currCost is None:
                            currCost = testPlan.cost(estimated=True)

                        self.addPlanCost(plan, currCost)


                        #now see if currCost better than minimum
                        #if so, update selectPlan and minimum cost
                        if minimum is None or (minimum > currCost):
                            selectedPlan = testPlan
                            minimum = cost


            temp = ','.join(str(t) for t in currOrder)


            if selectedPlan is None:
                plans[temp] = None
            else:
                plans[temp] = selectedPlan.root

        numTables = numTables + 1

    temp = ','.join(str(t) for t in tableId)
    return plans[temp]















