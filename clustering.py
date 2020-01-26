# Importing Modules
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.preprocessing import LabelEncoder
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import silhouette_score
#from sklearn.model_selection import train_test_split
import matplotlib.pyplot as plt
#import seaborn as sns
import pandas as pd
import numpy as np
from numpy import diff

#---START----
df = pd.read_csv('{PATH}')
df['DEVICE_TYPE'] = df['DEVICE_TYPE'].apply(lambda x: x.replace('mobile web', 'mobile_web'))
df['DEVICE_TYPE'] = df['DEVICE_TYPE'].apply(lambda x: x.replace('mobile app', 'mobile_app'))

#label encoder
pple = LabelEncoder()
df['DEVICE_TYPE'] = pple.fit_transform(df['DEVICE_TYPE'])
df['SIZE'] = pple.fit_transform(df['SIZE'])

'''df_scaled = df.copy()
cols = ['IMPRESSIONS', 'REVENUE']
features = df_scaled[cols]
scaler = StandardScaler().fit(features.values)
features = scaler.transform(features.values)
df_scaled[cols] = features
'''

#---FIND CORRECT AMOUNT OF COMPONENTS----
df.columns.values
features = ['DEVICE_TYPE', 'SIZE', 'MKT_REQUESTS', 'UNFILTERED_RATE', 'IMPRESSIONS', 'WIN_RATE', 'SPEND', 'REVENUE']
x = df.loc[:, features].values
x = StandardScaler().fit_transform(x)

pca_list = []

for n_components in range(2, 9):
    pca = PCA(n_components = n_components)
    components = pca.fit_transform(x)
    pca_variance = sum(pca.explained_variance_ratio_)
    pca_list.append(pca_variance)
    print("For n_components = {}, explained variance ratio is {}".format(n_components, pca_variance))

dx = 1
y = pca_list
dy = diff(y)/dx
print(dy)

pca_list_diff = []

for w in range(1, len(dy)):
    change = 1 - ((1 - y[w]) / (1 - y[w - 1]))
    pca_list_diff.append(change)

pca_list_diff_abs = [abs(num) for num in pca_list_diff]
n = pca_list_diff_abs.index(max(pca_list_diff_abs)) + 2

pca_deriv, ax = plt.subplots()
ax.plot(range(0, len(dy)), dy)
ax.set_xlabel('Number of components')
ax.set_ylabel('dy(explained variance)/dx')
plt.show()

#----FINDING RIGHT AMOUNT OF CLUSTERS----
pca = PCA(n_components= 3)
components = pca.fit_transform(x)
pca_df = pd.DataFrame(components, columns = ['pc1', 'pc2', 'pc3'])#, 'pc4', 'pc5', 'pc6'])
pca_df['PUBLISHER_ACCOUNT_NK'] = df['PUBLISHER_ACCOUNT_NK']
pca_df = pca_df[['PUBLISHER_ACCOUNT_NK', 'pc1', 'pc2', 'pc3']]#, 'pc4', 'pc5', 'pc6']]
print(pca.explained_variance_ratio_)
print(sum(pca.explained_variance_ratio_))

z = np.column_stack((pca_df['pc1'], pca_df['pc2'], pca_df['pc3']))#, pca_df['pc3'], pca_df['pc4'], pca_df['pc5'], pca_df['pc6']))
silhouette = []

for n_clusters in range(2, 21):
    kmeans = KMeans(n_clusters= n_clusters)
    labels = kmeans.fit_predict(z)
    centroids = kmeans.cluster_centers_
    score = silhouette_score(z, labels)
    silhouette.append(score)
    print("For n_clusters = {}, silhouette score is {}".format(n_clusters, score))

silhouette_diff = []

for i in range(1, len(silhouette)):
    improvement = 1 - ((1 - silhouette[i]) / (1 - silhouette[i - 1]))
    silhouette_diff.append(improvement)
    print("For n_cluster = {}, percent improvement = {}".format(i + 2, improvement))

silhouette_diff_abs = [abs(num) for num in silhouette_diff]
m = silhouette_diff_abs.index(max(silhouette_diff_abs)) + 3

kmeans = KMeans(n_clusters = 3)
kmeans.fit(z)
y_kmeans = kmeans.predict(z)

'''for i, j in zip(df_scaled2['PUBLISHER_ACCOUNT_NK'], y_kmeans):
    print(i, j)'''

plt.style.use('fivethirtyeight')

pts_usg_clustered, ax = plt.subplots()

cluster_1 = []
cluster_2 = []
cluster_3 = []
#cluster_4 = []

for i in range(len(y_kmeans)):
    if(y_kmeans[i] == 0):
        cluster_1.append(z[i])
    elif(y_kmeans[i] == 1):
        cluster_2.append(z[i])
    elif(y_kmeans[i] == 2):
        cluster_3.append(z[i])
'''    elif(y_kmeans[i] == 3):
        cluster_4.append(z[i])'''
        
cluster_1 = np.vstack(cluster_1)
cluster_2 = np.vstack(cluster_2)
cluster_3 = np.vstack(cluster_3)
#cluster_4 = np.vstack(cluster_4)

ax.scatter(cluster_1[:, 0], cluster_1[:, 1], label = "Cluster_1", c = "red")
ax.scatter(cluster_2[:, 0], cluster_2[:, 1], label = "Cluster_2", c = "blue")
ax.scatter(cluster_3[:, 0], cluster_3[:, 1], label = "Cluster_3", c = 'green')
#ax.scatter(cluster_3[:, 0], cluster_3[:, 1], label = "Cluster_4", c = 'green')

centroids = kmeans.cluster_centers_

ax.scatter(centroids[:, 0], centroids[:, 1], c = 'black', s = 200, alpha = .3, label = 'Cluster center')

ax.legend(loc='best', prop={'size': 12, "family": "Arial"})
ax.set_xlabel('pc1')
ax.set_ylabel('pc2')

plt.show()