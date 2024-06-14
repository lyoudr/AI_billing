from sklearn.metrics import r2_score
from scipy import stats
from typing import Tuple
import matplotlib.pyplot as plt 
import numpy 

"""
service_name : number mapping
mappings = {
  'App Engine': 1,
  'Compute Engine': 2,
  'Networking': 3,
  'BigQuery': 4
}
"""


# ----------------------------------- Polynomial Regression ---------------------------------
def predict_future_cost(x: list, y: list) -> Tuple[float, float]:
    # NumPy has a method that lets us make a polynomial model:
    mymodel = numpy.poly1d(numpy.polyfit(x, y, 3))
    # Then specify how the line will display, we start at position 1, and end at position 4
    myline = numpy.linspace(1, 4, 4)
    # Draw the original scatter plot:
    # plt.scatter(x, y)
    # Draw the line of polynomial regression:
    # plt.plot(myline, mymodel(myline))

    # The same still want to find the relationship between x and y 
    # using R-square
    print('second relationship r2_score is ->', r2_score(y, mymodel(x)))

    # from the r-squared value is equal to 1 , we see this model is very suitable for predicting future values
    # For example, we can predict the future cost of "Compute Engine"
    future_cost = mymodel(2)
    print('second future_cost is ->', future_cost)
    return future_cost, r2_score(y, mymodel(x))
