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


class GreedyOptimizer(Optimizer):
  def pickJoinOrder(self, plan):


    #numTables = 2
    typesOfJoins = ["nested-loops", "block-nested-loops"]
    count = 0
    tableId = []
    joinOperators = []
    #operators = {}
    #test = {}
    plans = {}
    fields = {}


    joinOp = None
    for (rand, operator) in plan.flatten():
        #joinOp = None
        if joinOp is None and not isinstance(operator, Join) and isinstance(operator.subPlan, Join) and not isinstance(operator, Join) and not isinstance(operator, TableScan):
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
            currIDStr = str(operator.subPlan.id())
            if currIDStr not in plans:
                currID = operator.id()
                tableId.append(currID)
                plans[str(currID)] = operator
                fields[currID] = operator.schema().fields


    tableIdList = []
    for ID in tableId:
        tableIdList.append(str(ID))

    numTables = len(tableIdList)

    '''
    want to select cheapest join possible
    '''
    while numTables > 1:
        minimum = None
        selectedPlan = None
        opLeftId = None
        opRightId = None

        combos = itertools.combinations(tableIdList, 2)

        for currOrder in combos:
            leftId = currOrder[0]
            rightId = currOrder[1]

            leftOp = None
            rightOp = None

            if leftId in plans:
                leftOp = plans[leftId]
            if rightId in plans:
                rightOp = plans[rightId]

            if leftOp is None or rightOp is None:
                continue


            #attributes of currJoin
            attributes = []
            currExpr = None

            '''
            #need to deal with any multi-way joins
            split on ,
            '''
            leftIds = leftId.split(",")
            rightIds = rightId.split(",")

            for ID in leftIds:
                intID = int(ID)
                attributes.extend(fields[intID])

            for ID in rightIds:
                intID = int(ID)
                attributes.extend(fields[intID])


            for join in joinOperators:
                expr = join.joinExpr
                if expr:
                    currAttributes = ExpressionInfo(join.joinExpr).getAttributes()
                    currSet = set(currAttributes)
                    attSet = set(attributes)
                if currSet.issubset(attSet):
                    currExpr = expr
                    break

            if currExpr is None:
                continue

            count = count + 2

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
                #Sampling involves way too much overhead....
                #taking too long to run :/
                #testPlan.sample(1.0)

                currCost = self.getPlanCost(plan)
                if currCost is None:
                    currCost = testPlan.cost(estimated=True)

                self.addPlanCost(plan, cost)


                #now see if currCost better than minimum
                #if so, update selectPlan and minimum cost
                if minimum is None or (minimum > currCost):
                    selectedPlan = testPlan
                    minimum = cost
                    opLeftId = leftId
                    opRightId = rightId


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

                self.addPlanCost(plan, cost)


                #now see if currCost better than minimum
                #if so, update selectPlan and minimum cost
                if minimum is None or (minimum > currCost):
                    selectedPlan = testPlan
                    minimum = cost
                    #switch left and right to match above
                    opLeftId = rightId
                    opRightId = leftId


        if selectedPlan is None:
            #blank statement to make logic work
            #pointless
            selectedPlan = selectedPlan
        else:
            key = opLeftId + "," + opRightId
            plans[key] = selectedPlan.root

            #remove everything at end
            #add final, eventually only the final will
            #remain in the list
            tableIdList.remove(opRightId)
            tableIdList.remove(opLeftId)
            tableIdList.append(key)
        numTables = numTables - 1

    if len(tableIdList) == 0:
        raise NotImplementedError

    return plans[tableIdList[0]]




