import os
import numpy as np
import matplotlib.pyplot as plt

IMAGES_PATH = os.path.join(os.path.dirname(os.getcwd()), "Data", "static")
os.makedirs(IMAGES_PATH, exist_ok=True)

def save_fig(fig_id, tight_layout=True, fig_extension="png", resolution=300):
    path = os.path.join(IMAGES_PATH, f"{fig_id}.{fig_extension}")
    print("Saving figure", fig_id)
    if tight_layout:
        plt.tight_layout()
    plt.savefig(path, format=fig_extension, dpi=resolution)
    plt.close()

def plot_bars(target_data, ref_data, fam, num):
    f, a, m = fam
    n_groups = len(target_data)

    means_frank = [float(i) for i in target_data.values()]
    means_guido = [float(i) for i in ref_data.values()]

    # create plot
    fig, ax = plt.subplots()
    index = np.arange(n_groups)
    bar_width = 0.35
    opacity = 0.8

    rects1 = ax.bar(index, means_frank, bar_width,
                    alpha=opacity,
                    color='b',
                    label='Married')

    rects2 = ax.bar(index + bar_width, means_guido, bar_width,
                    alpha=opacity,
                    color='g',
                    label='Unmarried')

    ax.set_xlabel(a)
    ax.set_ylabel(f"{f}({m})")
    ax.set_title(f"{m} by {a} for married and unmarried.")
    ax.set_xticks(index + bar_width)
    ax.set_xticklabels(target_data.keys(), rotation=45, ha="right")
    ax.legend()

    plt.tight_layout()
    save_fig(str(num))

def images(target_rows, ref_rows, fam, num):
    """
    Takes the target and reference tuples
    :param target_rows:
    :param ref_rows:
    :return:
    """
    plot_bars(dict(target_rows), dict(ref_rows), fam, num)

