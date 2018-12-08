import pydot
import numpy as np
import pandas as pd

import seaborn as sns
sns.set(style="whitegrid", color_codes=True)
import matplotlib.pyplot as plt

from sklearn import tree
from sklearn.metrics import recall_score, precision_score, f1_score, confusion_matrix, accuracy_score
from sklearn.ensemble import RandomForestClassifier
from sklearn.decomposition import PCA
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.neighbors import KNeighborsClassifier
from sklearn.model_selection import GridSearchCV


def plot_confusion_matrix(y_true, y_pred, filename):
    plt.figure()
    labels = ['exchanges', 'gambling', 'mixer', 'pool']
    mat = confusion_matrix(y_true, y_pred, labels)
    sns.heatmap(mat, square=True, annot=True, fmt='d', cmap="YlGnBu", cbar=True)
    plt.ylabel('true label'),
    plt.xlabel('predicted label')
    plt.savefig(filename+".pdf", bbox_inches = 'tight')
    plt.figure()
    mat_normalized = mat.astype('float') / mat.sum(axis=1)[:, np.newaxis]
    sns.heatmap(mat_normalized, fmt="f", square=True, annot=True, cmap="YlGnBu", cbar=True)
    plt.ylabel('true label'),
    plt.xlabel('predicted label')
    print("confusion matrix normalized: ")
    plt.tight_layout()
    plt.savefig(filename+"_normalized.pdf", bbox_inches = 'tight')

def get_best_dec_tree(X_train,y_train,X_test, y_test):
    best_acc = 0
    best_depth = 0
    for i in range(3,20):
        dec_tree = tree.DecisionTreeClassifier(max_depth = i, random_state=42, class_weight='balanced')
        dec_tree = dec_tree.fit(X_train, y_train)
        y_pred = dec_tree.predict(X_test)
        acc_score = accuracy_score(y_test, y_pred)
        if acc_score > best_acc:
            best_acc = acc_score
            best_depth = i
    print(best_depth)
    dec_tree = tree.DecisionTreeClassifier(max_depth = best_depth, random_state=42,  class_weight="balanced")
    dec_tree.fit(X_train, y_train)
    y_pred = dec_tree.predict(X_test)
    print("the accuracy = " + str(accuracy_score(y_test, y_pred)))
    return dec_tree, y_pred

def get_best_estimator_params(estimator, param_grid, X,Y):
    grid = GridSearchCV(estimator, param_grid, cv=10, scoring='accuracy', return_train_score=False, n_jobs = -1)
    grid.fit(X, Y)
    #df = pd.DataFrame(grid.cv_results_)[['mean_test_score', 'std_test_score', 'params']]
    grid_mean_scores = grid.cv_results_['mean_test_score']
    # plot the results
    plt.plot(param_grid[list(param_grid.keys())[0]], grid_mean_scores)
    plt.xlabel('Parameter')
    plt.ylabel('Cross-Validated Accuracy')

    return grid.best_params_


def plot_dec_tree(decision_tree, feature_names, filename):
    dot_data = tree.export_graphviz(decision_tree, out_file=None,
                                feature_names=feature_names,
                                filled=True,
                                rounded=True)
    graph = pydot.graph_from_dot_data(dot_data)
    graph[0].write_png(filename+".png")


def scores(y_test, y_pred):
    print('Accuracy:',accuracy_score(y_test, y_pred))
    print('Recall:',recall_score(y_test, y_pred, average=None))
    print('Precision:',precision_score(y_test, y_pred, average=None))
    print('f1 score:', f1_score(y_test, y_pred, average=None))

def get_best_random_forest(X_train,y_train,X_test, y_test):
    best_acc = 0
    best_depth = 0
    for i in range(3,40):
        rf = RandomForestClassifier(max_depth=i, random_state=42, class_weight="balanced")
        rf.fit(X_train, y_train)
        y_pred = rf.predict(X_test)
        acc_score = accuracy_score(y_test, y_pred)
        if acc_score > best_acc:
            best_acc = acc_score
            best_depth = i
    rf = RandomForestClassifier(max_depth=best_depth, random_state=42,  class_weight="balanced")
    rf.fit(X_train, y_train)
    y_pred = rf.predict(X_test)
    print("accuracy = " + str(accuracy_score(y_test, y_pred)))
    return rf, y_pred

def pca_and_plot(X_train, y_train):
    pca = PCA(n_components=2)
    principalComponents = pca.fit_transform(X_train)
    y_df = y_train.to_frame()
    y_df = y_df.reset_index()
    y_df = y_df.drop(columns=['user'])
    principalDf = pd.DataFrame(data = principalComponents, columns = ['principal component 1', 'principal component 2'])
    finalDf = pd.concat([principalDf, y_df], axis=1)
    # 2d plot:
    fig = plt.figure(figsize = (8,8))
    ax = fig.add_subplot(111)
    ax.set_xlabel('Principal Component 1')
    ax.set_ylabel('Principal Component 2')
    ax.set_title('PCA visualisation of user features')
    targets = ['exchanges','gambling','pool','mixer']
    colors = ['gray', 'red','blue', 'green']
    markers = ['^','o','s', "D"]
    tar = {'exchanges':0,'gambling':1,'pool':2,'mixer':3}
    for target, color in zip(targets,colors):
        indicesToKeep = finalDf['category'] == target
        ax.scatter(finalDf.loc[indicesToKeep, 'principal component 1']
                   , finalDf.loc[indicesToKeep, 'principal component 2']
                   , c = color
                   , marker=markers[tar[target]]
                   , s = 40)
    ax.legend(targets)
    ax.grid()
    fig.tight_layout()
    fig.savefig("pca_knn.png", bbox_inches = 'tight')

def normalize(X):
    # have to scale the data s.t. the relative variance can be distinguished for PCA
    X_min_max = MinMaxScaler().fit_transform(X)
    X_standard = StandardScaler().fit_transform(X)

    return X_min_max, X_standard

def get_best_pca_components(X_train, X_test, y_train, y_test):
    best_acc = 0
    best_n_components = 0
    best_n_neighbours = 0
    for i in range(1,X_train.shape[1]):
        pca = PCA(n_components=i)
        principalComponents = pca.fit_transform(X_train)
        principalComponentsTest = pca.fit_transform(X_test)
        for j in range(1,10):
            knn = KNeighborsClassifier(n_neighbors=j)
            knn.fit(principalComponents, y_train)
            y_pred = knn.predict(principalComponentsTest)
            acc = accuracy_score(y_test, y_pred)
            if acc > best_acc:
                best_acc = acc
                best_n_components = i
                best_n_neighbours = j
    pca = PCA(n_components=best_n_components)
    principalComponents = pca.fit_transform(X_train)
    principalComponentsTest = pca.fit_transform(X_test)
    knn = KNeighborsClassifier(n_neighbors=best_n_neighbours)
    knn.fit(principalComponents, y_train)
    y_pred = knn.predict(principalComponentsTest)
    print(accuracy_score(y_test, y_pred))
    print(best_n_components,best_n_neighbours)
    return knn, pca, principalComponents, principalComponentsTest, y_pred

def class_hist(df):
    x = list(df.category.value_counts().to_frame().index)
    cats = []
    for i in x:
        cats.append(np.sum(df["category"] == i))
    fig, ax = plt.subplots()
    plt.bar(range(0,len(cats)), cats)
    ax.set_xticks(range(0,len(cats)))
    for p in (ax.patches):
        ax.annotate(str(p.get_height()), (p.get_x()+0.34, p.get_height() +100), fontsize=10)
    ax.set_xticklabels(x)
    ax.set_ylabel('Number of users')
    ax.set_title('Distribution of classes')

def feature_plot(features, importances):
    indices = np.argsort(importances)
    cols = [features[i] for i in indices]
    pd.Series(importances[indices], index=cols).plot.bar(color='steelblue', figsize=(12, 6))
