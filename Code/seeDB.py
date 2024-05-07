import os
import csv
import numpy as np
from scipy import stats
import itertools
import time
import collections
import math
from functools import reduce
import plotting as p
import psycopg2 as ps
import psycopg2.extras as e

DATA_PATH = os.path.join(os.path.dirname(os.getcwd()), "Data/splits/")
FUNCTION_LIST = ["avg", "sum", "min", "max", "count"]
DIMENSIONS = ["workclass", "education", "occupation", "relationship", "race", "sex", "native_country", "economic_indicator"]
MEASURES = ["age", "fnlwgt", "hours_per_week", "capital_gain", "capital_loss"]


def get_database_connection():
    return ps.connect("dbname='seeDB' user='ahrs' host='localhost' password=''")


def execute_query(connection, command):
    cur = connection.cursor()
    cur.execute(command)
    rows = cur.fetchall()
    cur.close()
    return np.asarray(rows)


def create_tables(connection):
    cur = connection.cursor()
    for i in range(10):
        command = "create table census_{} (age real, workclass text, fnlwgt real, education text, education_num real, " \
                  "marital_status text, occupation text, relationship text, race text, sex text, capital_gain real, " \
                  "capital_loss real, hours_per_week real, native_country text, economic_indicator text)".format(i + 1)
        cur.execute(command)
    cur.close()
    connection.commit()


def insert_data(connection):
    cur = connection.cursor()
    for i in range(1, 11):
        with open(os.path.join(DATA_PATH, "test_{}.csv".format(i)), 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip the header row.
            for row in reader:
                cur.execute("INSERT INTO census_{} VALUES (%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s, %s,%s, %s, %s)".format(i), row)

    cur.close()
    connection.commit()


def create_views(connection):
    cur = connection.cursor()
    for i in range(10):
        command = """ create view married_{} as select * from census_{} where marital_status in 
        (' Married-AF-spouse', ' Married-civ-spouse', ' Married-spouse-absent',' Separated');
        create view unmarried_{} as select * from census_{} where marital_status in 
        (' Never-married', ' Widowed',' Divorced');""".format(i + 1, i + 1, i + 1, i + 1)
        cur.execute(command)
    cur.close()
    connection.commit()


def query_plotter(connection, utilities):
    cur = connection.cursor()
    for num, (kld, fam) in enumerate(utilities):
        f, a, m = fam
        command_target = "select {},{}({}) from married group  by {}".format(a, f, m, a)
        command_reference = "select {},{}({}) from unmarried group  by {}".format(a, f, m, a)
        target_tuples = execute_query(connection, command_target)
        reference_tuples = execute_query(connection, command_reference)
        p.images(target_tuples, reference_tuples, fam, num)


class SeeDB(object):
    def __init__(self, k):
        self.seen = collections.defaultdict(float)
        self.prune = []
        self.phase_count = 1
        self.top = k
        self.delta = 0.05
        self.triple_count = 0
        self.connection = get_database_connection()

    def __del__(self):
        self.connection.close()

    def share_opt_phase(self):
        print("Phase %d :" % self.phase_count)
        t0 = time.time()
        for (f, a, m) in itertools.product(FUNCTION_LIST, DIMENSIONS, MEASURES):
            if (f, a, m) not in self.prune:
                command_target = "select {},{}({}) from married_{} group  by {}".format(a, f, m, self.phase_count, a)
                command_reference = "select {},{}({}) from unmarried_{} group  by {}".format(a, f, m, self.phase_count, a)
                target_tuples = execute_query(self.connection, command_target)
                reference_tuples = execute_query(self.connection, command_reference)
                self.seen[(f, a, m)] = self.seen[(f, a, m)] * (1 - 1 / self.phase_count) + \
                                       modified_KL_forequal(target_tuples, reference_tuples) / self.phase_count
                self.triple_count += 1
        print("Computed %d triples" % self.triple_count)
        t1 = time.time()
        print('Time taken by this phase: %0.3f' % (t1 - t0))
        self.phase_count += 1

    def prune_opt(self):
        while self.phase_count <= 10:
            utilities = []
            self.share_opt_phase()
            self.mu = sum(self.seen.values()) / len(self.seen)
            m = self.phase_count
            min_threshold = min([_[1] for _ in sorted(self.seen.items(), key=lambda x: -x[1])[:self.top]])
            epsilon = math.sqrt((0.5 / m) * (1 - ((m - 1) / 10)) * (2 * math.log(math.log(m)) +
                                                                  math.log((math.pi ** 2) / (3 * self.delta))))
            for k, v in self.seen.items():
                if v + epsilon < min_threshold - epsilon:
                    self.prune.append(k)
                else:
                    utilities.append((v, k))
        return sorted(utilities)[::-1][:self.top]


def modified_KL_forequal(rows1, rows2):
    x = dict([tuple(_) for _ in rows1.tolist()])
    y = dict([tuple(_) for _ in rows2.tolist()])

    if len(y) > len(x):
        d = y
    else:
        d = x
    x = collections.defaultdict(lambda: np.finfo(float).eps, x)
    y = collections.defaultdict(lambda: np.finfo(float).eps, y)

    z = [(float(x[id]), float(y[id])) for id in set(x) & set(y)]
    p, q = zip(*z)
    pk = np.asarray(list(p))
    qk = np.asarray(list(q))
    pk = 1.0 * pk / np.sum(pk, axis=0)
    qk = 1.0 * qk / np.sum(qk, axis=0)
    res = pk * np.log(pk / qk)
    res[np.isneginf(res)] = 0.0
    res[np.isposinf(res)] = 0.0
    return np.sum(np.nan_to_num(res))


if __name__ == "__main__":
    create_tables(get_database_connection())
    insert_data(get_database_connection())
    create_views(get_database_connection())
    s = SeeDB(5)
    plot_utils = s.prune_opt()
    query_plotter(s.connection, plot_utils)