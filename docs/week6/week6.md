# [](#header-1)Week 6

## [](#header-2)Literature Review

### [](#header-3)Gjoreski et al., Unsupervised Online Activity Discovery Using Temporal Behaviour Assumption

This paper presents an online clustering algorithm that does not statically determine the number of clusters, 
as compared to Kmeans. Also it has constant time complexity and memory efficient as the data is processed in
real time using a sliding window. The data used in the experiments are activites like walking, lying, sitting,
cycling, etc. The features extracted are mean, median, standard deviation, energy, integral, skewness and 
kurtosis from 3 acceleration axis and magnitude of acceleration vector from accelerometers.
