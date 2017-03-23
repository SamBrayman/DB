import matplotlib.pyplot as plt
import numpy as np
plt.xlabel('Query Plans')
plt.ylabel('Elapsed Time')
plt.title('Comparison of Codebase to SQLite')

queries = ('1', '2', '3', '4')
y_pos = np.arange(len(queries))
performance = [0.004521608352661133,5.019667625427246,0,0]
performance2 = [0.0056836605072021484,4.999634265899658,0,0]
performance3 =  [0.163,0.849,2.180 ,2.180 ]
#Run Time: real 0.163 user 0.037595 sys 0.009321 Query 1
#Run Time: real 0.849 user 0.229636 sys 0.058009 Query 2
#Run Time: real 2.180 user 1.572201 sys 0.169648 Query 3
#query1 BNLJ
#Execution time: 0.004521608352661133
#query1 HASH
#Execution time: 0.0056836605072021484
#query2 BNLJ
#Execution time: 5.019667625427246
#query2 HASH
#Execution time: 4.999634265899658
#query3 BNLJ
#Execution time: 7251.6041469573975
#query3 HASH
#Execution time: 7222.663188339
#query4 BNLJ
#Execution time: 1404.3665747642517
#query4 HASH
#Execution time: 1398.761785244

bnlj = plt.bar(y_pos-.2, performance,width=.2, align='center', alpha=0.5,color='r',label="BNLJ")
hashj = plt.bar(y_pos, performance2,width=.2, align='center', alpha=0.5,color='b',label="HashJ")
sqlj = plt.bar(y_pos+.2, performance3,width=.2, align='center', alpha=0.5,color='g',label="SQLJ")
plt.xticks(y_pos, queries)
#plt.legend()
#handles, labels = plt.get_legend_handles_labels()
plt.legend(handles=[bnlj,hashj,sqlj])
#plt.set_xticklabels(['BNLJ','HashJ','SQLJ'])
plt.autoscale(tight=True)
plt.show()
plt.savefig("PYPLOT WITH SMALL VALUES")
