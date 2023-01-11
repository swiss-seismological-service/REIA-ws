# -*- coding: utf-8 -*-
"""
Created on Fri Oct 28 13:24:01 2022

@author: papadopoulos
"""
import numpy as np
from scipy.stats import beta

VALUES = np.array([72379.94,  2465.5142, 15776.068, 10078.892, 36903.223, 13494.167,
                   9598.837, 29200.87,  4241.889,  8662.3955, 46441.754,  9528.981,
                   2668.303,  4462.573,  2481.2524,  4483.8994, 119232.805,  3789.397,
                   6187.967, 15582.486,  4407.1133, 12258.019, 34259.535, 12024.441,
                   64813.074, 27495.611,  3988.4214, 94528.34,  6041.626, 34713.086,
                   53069.902,  2701.9778,  5388.111,  5615.3306,  8860.164,  7616.189,
                   61089.168, 14259.61, 113086.37,  7238.528,  2795.9448, 19252.242,
                   35075., 39118.04,  6292.3525, 106534.09,  2507.6934, 92436.77,
                   89457.17,  8797.484, 20350.32,  4797.642,  5981.469, 98960.68,
                   78348.18, 19336.91,  2777.4036,  4663.011,  9209.485])

WEIGHTS = np.array([0.0001, 0.0001, 0.0001, 0.0001,
                    0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
                    0.0001, 0.0001, 0.0001, 0.0001,
                    0.0001, 0.00015, 0.00015, 0.0001, 0.0001,
                    0.0001, 0.0001, 0.0001, 0.00015,
                    0.0001, 0.0001, 0.0001, 0.0001, 0.0001,
                    0.0001, 0.0001, 0.0001, 0.0003,
                    0.0003, 0.0003, 0.00045, 0.0003, 0.0003,
                    0.0003, 0.0003, 0.0003, 0.0003,
                    0.0003, 0.0003, 0.0003, 0.0003, 0.0003,
                    0.0003, 0.0003, 0.0003, 0.0003,
                    0.0003, 0.0003, 0.0003, 0.0003, 0.0003,
                    0.0003, 0.0003, 0.0003, 0.00045, 0.0003])


def add_missing_zeroes(values, weights):
    zero_weight = 1 - np.sum(weights)
    v = np.append(
        values, [0])
    w = np.append(
        weights, [zero_weight])
    return (v, w)


def weighted_quantile(values, quantiles, weights):
    """
    Calculates Quantiles of a weighted list of samples.
    Implementation for C=0 of:
    https://en.wikipedia.org/wiki/Percentile#The_weighted_percentile_method

    If the sum of the weights is smaller than 1, it is assumed that
    the data is sparse and the remaining data is filled up with 0.

    :param values: array-like with data
    :param quantiles: array-like with many quantiles needed
    :param weights: array-like of the same length as `array`
    :return: numpy.array with computed quantiles.
    """

    values = np.array(values)
    quantiles = np.array(quantiles)
    weights = np.array(weights)

    sum_weight = np.sum(weights)

    if sum_weight != 1 and sum_weight < 1:
        values, weights = add_missing_zeroes(values, weights)

    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), \
        'Quantiles should be in [0, 1]'

    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]

    weighted_quantiles = np.cumsum(weights)  # C=0

    if sum_weight != 0:
        weighted_quantiles /= np.sum(weights)

    print(weighted_quantiles, values)
    return np.interp(quantiles, weighted_quantiles, values)


def wquantile_generic(values, quantiles, cdf_gen, weights):
    """
    source: https://aakinshin.net/posts/weighted-quantiles/
    """
    values = np.array(values)
    quantiles = np.array(quantiles)
    weights = np.array(weights)

    sum_weight = np.sum(weights)

    if sum_weight != 1 and sum_weight < 1:
        values, weights = add_missing_zeroes(values, weights)

    nw = sum(weights)**2 / sum(weights**2)
    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]

    weights = weights / sum(weights)
    cdf_probs = np.cumsum(np.insert(weights, 0, [0]))
    res = []
    for prob in quantiles:
        cdf = cdf_gen(nw, prob)
        q = cdf(cdf_probs)
        w = q[1:]-q[:-1]
        res.append(np.sum(w*values))
    return res


def whdquantile(values, quantiles, weights):
    """
    source: https://aakinshin.net/posts/weighted-quantiles/
    """
    def cdf_gen_whd(n, p):
        return lambda x: beta.cdf(x, (n + 1) * p, (n + 1) * (1 - p))
    return wquantile_generic(values, quantiles, cdf_gen_whd, weights)


def type_7_cdf(quantiles, n, p):
    """
    source: https://aakinshin.net/posts/weighted-quantiles/
    """
    h = p * (n - 1) + 1
    u = np.maximum((h - 1) / n, np.minimum(h / n, quantiles))
    return u * n - h + 1


def wquantile(values, quantiles, weights):
    """
    source: https://aakinshin.net/posts/weighted-quantiles/
    """
    def cdf_gen_t7(n, p):
        return lambda x: type_7_cdf(x, n, p)
    return wquantile_generic(values, quantiles, cdf_gen_t7, weights)


if __name__ == '__main__':
    q10, q90 = weighted_quantile(VALUES, (0.1, 0.9), WEIGHTS)
    print(q10, q90)
    q10, q90 = whdquantile(VALUES, (0.1, 0.9), WEIGHTS)
    print(q10, q90)
    q10, q90 = wquantile(VALUES, (0.1, 0.9), WEIGHTS)
    print(q10, q90)
