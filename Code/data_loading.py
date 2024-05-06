import pandas as pd
import os
import numpy as np
import time

class Solution:
    def __init__(self, splits):
        self.splits = splits

    @classmethod
    def load_data(cls, file_path):
        return pd.read_csv(file_path, sep=",")

    def split_data_and_save(self, dataframe_all, data_path):
        t0 = time.time()
        df_split = np.array_split(dataframe_all, self.splits)
        for i, split_df in enumerate(df_split, 1):
            split_df.to_csv(os.path.join(data_path, f"test_{i}.csv"), encoding='utf-8', index=False)
        t1 = time.time()
        print('Time taken by this process: %0.2f' % (t1 - t0))


if __name__ == "__main__":
    DATA_PATH = os.path.join(os.path.dirname(os.getcwd()), "Data/splits/")
    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)

    file_path = os.path.join(os.path.dirname(os.getcwd()), "Data/processed_data.txt")
    data = Solution(10)
    dataframe_all = data.load_data(file_path)
    data.split_data_and_save(dataframe_all, DATA_PATH)
