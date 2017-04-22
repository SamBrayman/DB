Sam Brayman - sbrayma1
Michael Oluwole - moluwol1

Design Choices:

We didn't do the histrograms. We did the bushy tree join and the greedy optimizer. 
The cost models use the simple equations from lecture more or less.
Experiments were run on the .001 data set.

Experiment Results
Query1 
The optimizer and the regular approach run at basically the same speed. The algorithm takes into account
queries on 1 relation.

Query2
The optimizer runs slightly slower than the regular approach likely due to the fact that the optimizer
does unnecessary work by considering the join ordering of the two relations. The ordering of the two
relations doesn't seem to be as impactful likely due to similar numbers of tuple and selectivity of the relations.

Query3
The optimizer runs faster than the nonoptimized. This is likeyly because of skewed selectivity and number of tuples
between the relations in this query. This skewing causes an optimal join ordering to be more effective than a naive approach.

Query4
The optimizer runs slower than the nonoptimized version, but not unsurprisingly. The optimizer does take time to analyze
the larger number of queries in order to find the join ordering, but after that it runs smoothly. The relations probably didn't have as much skew in ordering, so an optimizer wouldn't help as much as expected. The optimizer doesn't take an unreasonable amount of time to perform the query though.

Query5
The optimizer does orders of magnitude better than the regular approach. 
Due to the large number of joins, an optimal join ordering would ensure that the query doesn't take as long. 
A naive approach is likely unreasonable considering the number of relationsthat are invovled in the query.

Note about the 416 part:

We attempted to implement the Greedy and Bushy Optimizers.

Here are the times for testing Bushy and Greedy.  
We want to quickly note that we had issues running the tests on the ugrad server- ugrad was running very slowly and our connections disconnected several times while running.  
We think this might have caused our times to be relatively slow.  
However, relative to each other the times should be ok because they were all run at the same time, on the same slow connection.

The test are available in the TestBushyGreedy.py. 
We didn't have enough time to run tquery12 so we only have times for queries 2, 4, 6, 8, and 10. 
Greedy was always faster than bushy. Based on the trend from the results we predict that query12 would show that Greedy was still faster than Bushy:


Greedy Times:
Number of plans considered: 2
Time 2 plans: 1.1244184970855713

Number of plans considered: 12
Time 4 plans: 37.92511820793152

Number of plans considered: 30
Time 6 plans: 75.814532208143328

Number of plans considered: 56
Time 8 plans: 125.21490712178901

Number of plans considered: 90
Time 10 plans: 210.6570880221302

Bushy Times:
Number of plans considered: 4
Time 2 plans: 1.2335996627807617

Number of plans considered: 16
Time 4 plans: 90.34067583084106

Number of plans considered: 28
Time 6 plans: 133.56815671920776

Number of plans considered: 138
Time 8 plans: 213.24553212234981

Number of plans considered: 154
Time 10 plans: 261.1965239811202
