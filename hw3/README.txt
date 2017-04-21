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
the larger number of queries in order to find the join ordering, but after that it runs smoothly. The relations probably didn't
have as much skew in ordering, so an optimizer wouldn't help as much as expected. The optimizer doesn't take an unreasonable amount
of time to perform the query though.
Query5
The optimizer does orders of magnitude better than the regular approach. Due to the large number of joins, an optimal join ordering 
would ensure that the query doesn't take as long. A naive approach is likely unreasonable considering the number of relations
that are invovled in the query.

Note about the 416 part:

We attempted to implement the Greedy and Bushy Optimizers (the files are included in here)_.  Unfortunately we didn't have enough time to run our testing scripts on them.  However we predict that the Greedy will be faster than the Bushy because of how it deals with the cheapest subplan.  Additionally the bushy will consider more more plans, taking more time than the Greedy.
