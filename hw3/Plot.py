import matplotlib.pyplot as plt
import numpy as np
plt.xlabel('Query Plans')
plt.ylabel('Elapsed Time (s)')
plt.title('Comparison of Optimization to Non-Optimization')
'''
query1
Execution time: 0.43109703063964844
query1 Optimized
Execution time: 0.44269347190856934
query2
Execution time: 0.3919703960418701
query2 Optimized
Execution time: 1.1067113876342773
query3
Execution time: 21.9739191532135
query3 Optimized
Execution time: 4.479973077774048
query4
Execution time: 0.038863182067871094
query4 Optimized
Execution time: 4.2490010261535645
query5
Execution time: 1097.5430915355682
query5 Optimized
Execution time: 7.205395698547363
'''
queries = ('1', '2', '3', '4','5')
y_pos = np.arange(len(queries))
performance = [0.43109703063964844,0.3919703960418701,21.9739191532135,0.038863182067871094,1097.5430915355682]
performance2 = [0.44269347190856934,1.1067113876342773,4.479973077774048,4.2490010261535645,7.205395698547363]



opt = plt.bar(y_pos-.1, performance,width=.2, align='center', alpha=0.5,color='r',label="NonOptimized")
nonopt = plt.bar(y_pos+.1, performance2,width=.2, align='center', alpha=0.5,color='b',label="Optimized")
plt.xticks(y_pos, queries)
#plt.legend()
#handles, labels = plt.get_legend_handles_labels()
plt.legend(handles=[opt,nonopt])
#plt.set_xticklabels(['BNLJ','HashJ','SQLJ'])
plt.autoscale(tight=True)
plt.show()
#plt.savefig("PYPLOT WITH SMALL VALUES.png")
