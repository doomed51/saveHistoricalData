from matplotlib.ticker import Formatter

"""
    This formatter is meant to help skip weekends when plotting a timeseries.

"""

class MyFormatter(Formatter):
    def __init__(self, dates, fmt='%a'):
        self.dates = dates
        self.fmt = fmt

    def __call__(self, x, pos=0):
        """Return the label for time x at position pos."""
        try:
            # return self.dates[round(x)].item().strftime(self.fmt)
            return self.dates[int(x)].strftime(self.fmt)
        except IndexError:
            pass