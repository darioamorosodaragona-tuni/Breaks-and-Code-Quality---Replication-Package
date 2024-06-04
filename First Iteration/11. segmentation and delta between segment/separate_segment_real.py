import os
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt


# Function to compute delta for a given segment
def compute_delta(segment1_X, segment1_Y, segmentX, segmentY):
    return segment1_Y[0] - np.interp(segment1_X[0], segmentX, segmentY)

# Iterate over files in directory
directory = "../piecewise5_convergent"
output_dir = "../piecewise5_computed_results"
plot_dir = "../piecewise5_computed_results_plot"
os.makedirs(output_dir, exist_ok=True)
os.makedirs(plot_dir, exist_ok=True)



for file_name in os.listdir(directory):
    file_path = os.path.join(directory, file_name)
    output_file_path = os.path.join(output_dir, f"computed_{file_name}")

    # Read file
    with open(file_path, 'r') as file:
        lines = file.readlines()[1:]  # Skip header line

    # Process each line in the file
    computed_results = []
    for line in lines:
        # Process line to extract data (assuming comma-separated values)
        data = line.strip().split(',')
        metric = data[0]
        Xsolution = [float(x) for x in data[1].strip('[]').split()]
        Ysolution = [float(y) for y in data[2].strip('[]').split()]
        breakpoints = [float(bp) for bp in data[3].strip('[]').split()]

        try:
            mse = np.format_float_scientific(float(data[4]), precision=30)
        except:
            continue

        iteration = float(data[5])

        # Separate segments
        segment1_X = Xsolution[:2]
        segment1_Y = Ysolution[:2]

        segment2_X = Xsolution[1:3]
        segment2_Y = Ysolution[1:3]

        segment3_X = Xsolution[2:4]
        segment3_Y = Ysolution[2:4]

        segment4_X = Xsolution[3:]
        segment4_Y = Ysolution[3:]

        # Translate segments to start at x=1
        start_X_segment1 = segment1_X[0]

        # Calcola il valore di y per x=0
        m2, b2 = np.polyfit(segment2_X, segment2_Y, 1)
        y2_for_x_0 = m2 * start_X_segment1 + b2
        segment2_X_equation = [start_X_segment1] + segment2_X
        segment2_Y_equation = [y2_for_x_0] + segment2_Y

        m3, b3 = np.polyfit(segment3_X, segment3_Y, 1)
        y3_for_x_0 = m3 * start_X_segment1 + b3
        segment3_X_equation = [start_X_segment1] + segment3_X
        segment3_Y_equation = [y3_for_x_0] + segment3_Y

        m4, b4 = np.polyfit(segment4_X, segment4_Y, 1)
        y4_for_x_0 = m4 * start_X_segment1 + b4
        segment4_X_equation = [start_X_segment1] + segment4_X
        segment4_Y_equation = [y4_for_x_0] + segment4_Y

        # Plot
        # plt.figure(figsize=(10, 6))
        # plt.plot(segment1_X, segment1_Y, marker='o', linestyle='-', color='b', label='Segment 1')
        # plt.plot(segment2_X_equation, segment2_Y_equation, marker='o', linestyle='-', color='g', label='Segment 2')
        # plt.plot(segment3_X_equation, segment3_Y_equation, marker='o', linestyle='-', color='r', label='Segment 3')
        # plt.plot(segment4_X_equation, segment4_Y_equation, marker='o', linestyle='-', color='c', label='Segment 4')
        #
        # plt.xlabel(f'X ({metric})')
        # plt.ylabel('Y (Solution)')
        # plt.title(f'{metric} - MSE: {mse}, Iteration: {iteration}')
        # plt.legend()
        # plt.grid(True)
        # plt.savefig(f"{plot_dir}/segmented_{file_name.replace('.csv', '')}_{metric}.png")
        # plt.close()
        #
        # plt.figure(figsize=(10, 6))
        # plt.plot(Xsolution, Ysolution, marker='o', linestyle='-', color='b', label='Solution')
        # y_breakpoints = []
        # for bp in breakpoints:
        #     index = Xsolution.index(bp)
        #     y_breakpoints.append(Ysolution[index])
        # plt.scatter(breakpoints, y_breakpoints, label='Breakpoints')
        # plt.xlabel('X (Delta Added Lines)')
        # plt.ylabel('Y (Solution)')
        # plt.title(f'{metric} - MSE: {mse}, Iteration: {iteration}')
        # plt.legend()
        # plt.grid(True)
        # plt.savefig(f"{plot_dir}/{file_name.replace('.csv', '')}_{metric}.png")
        # plt.close()

        # Compute delta for each segment
        delta_segment2 = compute_delta(segment1_X, segment1_Y, segment2_X_equation, segment2_Y_equation)
        delta_segment3 = compute_delta(segment1_X, segment1_Y, segment3_X_equation, segment3_Y_equation)
        delta_segment4 = compute_delta(segment1_X, segment1_Y, segment4_X_equation, segment4_Y_equation)

        # Append results to list
        computed_results.append([metric, delta_segment2, delta_segment3, delta_segment4])

    # Write computed results to output file
    df = pd.DataFrame(computed_results, columns=["Metric", "Delta Segment 2", "Delta Segment 3", "Delta Segment 4"])
    df.to_csv(output_file_path, index=False)
