import io
import logging
import os
import sys

import pandas as pd
import numpy as np
import scipy
from numpy.linalg import lstsq
from tqdm import tqdm
import warnings

# Suppress the warning
# warnings.filterwarnings("ignore")
warnings.simplefilter(action='ignore', category=Warning)

ramp = lambda u: np.maximum(u, 0)
step = lambda u: (u > 0).astype(float)


def clustering(X, Y, nCluster=3):
    import matplotlib.pyplot as plt
    X = np.array(X)
    Y = np.array(Y)

    # Find unique values in commits_delta_date
    unique_values = np.unique(X)

    # Initialize a dictionary to store centroids
    centroids = {}

    # Calculate centroid for each unique value in commits_delta_date
    for value in unique_values:
        # Find indices where commits_delta_date equals the current value
        indices = np.where(X == value)[0]
        # print(indices)
        # Filter delta_complexity values
        delta_complexity_values = Y[indices]

        # Calculate centroid
        centroid = np.mean(delta_complexity_values)

        # Store centroid in dictionary
        centroids[value] = centroid

    # # Plot clusters and centroids
    # plt.figure(figsize=(10, 6))
    # for value, centroid in centroids.items():
    #     # Find indices where commits_delta_date equals the current value
    #     indices = np.where(X == value)[0]
    #
    #     # Plot data points in cluster
    #     # plt.scatter(commits_delta_date[indices], delta_complexity[indices], label=f'Cluster {value}')
    #
    #     # Plot centroid
    #     plt.scatter(value, centroid, color='red', marker='o', edgecolors='black', label=f'Centroid {value}')
    # plt.scatter(commits_delta_date, delta_complexity, label='Data Points')
    #
    # plt.xlabel('commits_delta_date')
    # plt.ylabel('delta_complexity')
    # plt.title('Clusters')
    # plt.legend()
    # plt.grid(True)
    # plt.show()
    return np.array(list(centroids.keys())), np.array(list(centroids.values()))


def remove_first_quartile(X, Y):
    X = np.asarray(X)
    Y = np.asarray(Y)
    Q1 = np.percentile(X, 25)
    lower_bound = Q1
    filtered_indices = np.where(X >= lower_bound)[0]
    X_filtered = X[filtered_indices]
    Y_filtered = Y[filtered_indices]
    return X_filtered, Y_filtered


def SegmentedLinearReg(X, Y, breakpoints, nIterationMax=10):
    breakpoints = np.sort(np.array(breakpoints))

    dt = np.min(np.diff(X))
    ones = np.ones_like(X)

    for i in range(nIterationMax):
        # Linear regression:  solve A*p = Y
        Rk = [ramp(X - xk) for xk in breakpoints]
        Sk = [step(X - xk) for xk in breakpoints]
        A = np.array([ones, X] + Rk + Sk)
        p = lstsq(A.transpose(), Y, rcond=None)[0]

        # Parameters identification:
        a, b = p[0:2]
        ck = p[2:2 + len(breakpoints)]
        dk = p[2 + len(breakpoints):]

        # Estimation of the next break-points:
        newBreakpoints = breakpoints - dk / ck

        # Stop condition
        if np.max(np.abs(newBreakpoints - breakpoints)) < dt / 5:
            break

        breakpoints = newBreakpoints

    # Compute the final segmented fit:
    Xsolution = np.insert(np.append(breakpoints, max(X)), 0, min(X))
    ones = np.ones_like(Xsolution)
    Rk = [c * ramp(Xsolution - x0) for x0, c in zip(breakpoints, ck)]

    Ysolution = a * ones + b * Xsolution + np.sum(Rk, axis=0)

    # Calculate residuals for each segment
    residuals = []
    for i in range(len(breakpoints) + 1):
        mask = (X >= Xsolution[i]) & (X <= Xsolution[i + 1])
        residuals.extend(
            Y[mask] - (a + b * X[mask] + np.sum([c * ramp(X[mask] - x0) for x0, c in zip(breakpoints, ck)], axis=0)))

    # Calculate mean squared error
    mse = np.mean(np.array(residuals) ** 2)

    # print(f'Mean Squared Error: {mse}')

    return Xsolution, Ysolution, breakpoints, mse


def merge_zeros(X, Y):
    merged_commits_delta_date = []
    merged_other = []

    i = 0
    while i < len(X):
        if X[i] != 0 or i == 0:
            # If current element is not zero, simply append to merged lists
            merged_commits_delta_date.append(X[i])
            merged_other.append(Y[i])
            i += 1
        else:
            # If current element is zero, find the group of consecutive zeros
            zeros_sum = 0
            num_zeros = 0
            while i < len(X) and X[i] == 0:
                zeros_sum += Y[i]
                num_zeros += 1
                i += 1

            # Calculate the average value for the group of consecutive zeros
            average_other = zeros_sum / num_zeros

            # Append the average value to the previous non-zero value in merged_other

            merged_other[-1] = average_other

            # Skip appending 0 to merged_commits_delta_date since it's already done
            # when the previous non-zero value was appended

    return merged_commits_delta_date, merged_other


OVERWRITE = True
OUT_DIR = 'piecewise5'
os.makedirs(OUT_DIR, exist_ok=True)
files = os.listdir('../files_per_package_with_metric_and_sonar_with_delta')

if not OVERWRITE:
    already_done = os.listdir(OUT_DIR)
    files = [file for file in files if file not in already_done]

files = [file for file in files if file.endswith('.csv')]

for file in tqdm(files, desc="files", position=0, leave=True):

    results = "metric,Xsolution,Ysolution,breakpoints,mse,iteration;\n"
    data = pd.read_csv(f'../files_per_package_with_metric_and_sonar_with_delta/{file}')
    delta_days = data['action_delta_date']
    cols = data.columns.tolist()
    cols = [x for x in cols if x.startswith('delta_')]
    for col in tqdm(cols, desc="cols", position=1, leave=False):
        Y = data[col]
        X = delta_days
        X, Y = merge_zeros(X, Y)
        X, Y = remove_first_quartile(X, Y)
        X, Y = clustering(X, Y)

        initialBreakpoints = [np.percentile(X, 25), np.percentile(X, 50),
                              np.percentile(X, 75)]

        iterations = [1, 10, 100, 1000]
        mse = None
        XsolutionBest = None
        YsolutionBest = None
        breakpointsBest = None
        iterationBest = None
        for iteration in iterations:
            try:
                Xsolution, Ysolution, breakpoints, mseSol = SegmentedLinearReg(X, Y, initialBreakpoints, iteration)
            except:
                continue
            if mse == None:
                mse = mseSol
                XsolutionBest = Xsolution
                YsolutionBest = Ysolution
                breakpointsBest = breakpoints
                iterationBest = iteration
            elif mseSol < mse:
                mse = mseSol
                XsolutionBest = Xsolution
                YsolutionBest = Ysolution
                breakpointsBest = breakpoints
                iterationBest = iteration

        XsolutionBest_str = XsolutionBest
        YsolutionBest_str = YsolutionBest
        breakpointsBest_str = breakpointsBest

        if breakpointsBest is not None:
            breakpointsBest_str = '[' + ' '.join(map(str, breakpointsBest)) + ']'
        if XsolutionBest is not None:
            XsolutionBest_str = '[' + ' '.join(map(str, XsolutionBest)) + ']'
        if YsolutionBest is not None:
            YsolutionBest_str = '[' + ' '.join(map(str, YsolutionBest)) + ']'

        results += f"{col},{XsolutionBest_str},{YsolutionBest_str},{breakpointsBest_str},{mse},{iterationBest}\n"

    with open(f'{OUT_DIR}/{file}', 'w') as f:
        f.write(results)
