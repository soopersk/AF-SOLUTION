FREQ_TO_CODE = {"M": "M", "MONTHLY": "M", "D": "D", "DAILY": "D"}

def normalize_frequency(freq):
    if freq is None:
        return None
    return FREQ_TO_CODE.get(freq.upper().strip())