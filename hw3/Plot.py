import matplotlib.pyplot as plt
import numpy as np
plt.xlabel('Query Plans')
plt.ylabel('Elapsed Time')
plt.title('Comparison of Optimization to Non-Optimization')

queries = ('1', '2', '3', '4','5')
y_pos = np.arange(len(queries))
performance = [0.004521608352661133,5.019667625427246,7251.6041469573975,1404.3665747642517,1000]
performance2 = [0.0056836605072021484,4.999634265899658,7222.663188339,1398.761785244,1000]



opt = plt.bar(y_pos-.1, performance,width=.2, align='center', alpha=0.5,color='r',label="Optimized")
nonopt = plt.bar(y_pos+.1, performance2,width=.2, align='center', alpha=0.5,color='b',label="NonOptimized")
plt.xticks(y_pos, queries)
#plt.legend()
#handles, labels = plt.get_legend_handles_labels()
plt.legend(handles=[opt,nonopt])
#plt.set_xticklabels(['BNLJ','HashJ','SQLJ'])
plt.autoscale(tight=True)
plt.show()
#plt.savefig("PYPLOT WITH SMALL VALUES.png")
