import numpy as np
from scipy.stats import beta


def add_missing_zeroes(values, weights):
    zero_weight = 1 - np.sum(weights)
    v = np.append(
        values, [0])
    w = np.append(
        weights, [zero_weight])
    return (v, w)


def weighted_quantile(values, quantiles, weights):
    """ Very close to numpy.percentile, but supports weights.
    NOTE: quantiles should be in [0, 1]!
    :param values: numpy.array with data
    :param quantiles: array-like with many quantiles needed
    :param sample_weight: array-like of the same length as `array`
    :param values_sorted: bool, if True, then will avoid sorting of
        initial array
    :param old_style: if True, will correct output to be consistent
        with numpy.percentile.
    :return: numpy.array with computed quantiles.

    @author: papadopoulos

    """

    values = np.array(values)
    quantiles = np.array(quantiles)
    weights = np.array(weights)

    sum_weight = np.sum(weights)
    if sum_weight != 1 and sum_weight < 1:
        values, weights = add_missing_zeroes(values, weights)

    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), \
        'quantiles should be in [0, 1]'

    sorter = np.argsort(values)
    values = values[sorter]
    weights = weights[sorter]

    weighted_quantiles = np.cumsum(weights) - 0.5 * weights

    if sum_weight != 0:
        weighted_quantiles /= np.sum(weights)

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
