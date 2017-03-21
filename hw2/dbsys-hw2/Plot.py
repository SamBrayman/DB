import matplotlib.pyplot as plt
import numpy as np
plt.xlabel('Query Plans')
plt.ylabel('Elapsed Time')
plt.title('Comparison of Codebase to SQLite')

queries = ('1', '2', '3', '4')
y_pos = np.arange(len(queries))
performance = [10,9,8,7]
performance2 = [1,5,6,7]
performance3 =  [5,5,5,5]
#Run Time: real 0.163 user 0.037595 sys 0.009321 Query 1
#Run Time: real 0.849 user 0.229636 sys 0.058009 Query 2
#Run Time: real 2.180 user 1.572201 sys 0.169648 Query 3
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
#plt.savefig()
