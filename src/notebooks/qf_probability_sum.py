import numpy as np
from scipy.stats import binom as binom_dist

def qf_probability_sum(num_slots:int|np.int64, num_elements:int|np.int64, fingerprint_length:int|np.int64) -> float:
    """
        This sum is well approximated by alpha*2^(-f) for alpha = num_elements / num_slots 
        :param num_slots: The number of slots in the filter
        :param num_elements: The number of elements to be inserted into the filter
        :param fingerprint_length: The length of the fingerprint used in the filter
        :return: The sum of probabilities
    """
    m = num_slots
    n = num_elements
    f = fingerprint_length
    assert isinstance(m, int) | isinstance(f, np.int64), "num_slots must be an integer"
    assert isinstance(n, int) | isinstance(f, np.int64), "num_elements must be an integer"
    assert isinstance(f, int) | isinstance(f, np.int64), "fingerprint_length must be an integer"
    dist = binom_dist(n, 1./m)
    probs = dist.pmf(np.arange(m+1))
    probs *= (1. - (1. - 2.**(-f))**np.arange(m+1))
    return np.sum(probs)