from sklearn.metrics import r2_score
from scipy import stats
from typing import Tuple
import matplotlib.pyplot as plt 
import numpy
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression

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


# ------------------------------------ Self-trained Model ----------------------------------
class AnnLinearRegression():
    def __init__(self, x: list, y:list):
        """
        In the equation of a line 𝑦 = 𝑚𝑥 + 𝑏
          𝑚 is the slope.
          𝑏 is the intercept.
        """
        self.X = x
        self.Y = y
        self.m = None
        self.b = None

    def define_m_b(self):
        self.m, self.b = np.polyfit(self.X, self.Y, 1)

    # Make predictions substituting the obtained slope and intercept 
    # coefficients into the equation Y = mx + b
    def model(self, X, b, m) -> list:
        Y = m * X + b
        return Y

    def loss_func(self, X, Y, b, m) -> float:
        loss = sum(
            (y - (b + m * x)) ** 2 for y, x in zip(Y, X)
        )
        return loss

    def test_data(self):
        X_test = np.array([50, 120, 280])
        Y_test = self.model(self.m, self.b, X_test)
        return X_test, Y_test

    def error_surface(self, m, b):
        m_values = np.linspace(m - 1, m + 1, 100)
        b_values = np.linspace(b - 1, b + 1, 100)
        M, B = np.meshgrid(m_values, b_values)
        Z = np.array([[self.loss_func(self.X, self.Y, b, m) for m in m_values] for b in b_values])

        fig = plt.figure()
        ax = fig.add_subplot(111, projection='3d')
        ax.plot_surface(M, B, Z, cmap='viridis')
        ax.set_xlabel('Slope (m)')
        ax.set_ylabel('Intercept (b)')
        ax.set_zlabel('Loss')
        plt.show()

    # Gradient Descent 
    def gradient_descent(self, m, b):
        SX = pd.Series(self.X)
        SY = pd.Series(self.Y)

        # partial derivative to m
        def partial_derivative_m(m: float) -> float:
            derivative = -2 * ((SY - (b + m * SX)) * SX).sum()
            return derivative

        # partial derivative to b
        def partial_derivative_b(b: float) -> float:
            derivative = -2 * ((SY - (b + m * SX))).sum()
            return derivative

        def recursive_find_best_m(old_m, der_fun, lr):
            if old_m <= 0.01:
                return old_m 
            new_m = old_m - lr * der_fun(old_m)
            return recursive_find_best_m(new_m, der_fun, lr)

        def recursive_find_best_b(old_b, der_fun, lr):
            if old_b <= 0.01:
               return old_b 
            new_b = old_b - lr * der_fun(old_b)
            return recursive_find_best_m(new_b, der_fun, lr)

        best_m = recursive_find_best_m(m, partial_derivative_m, lr=1)
        best_b = recursive_find_best_b(b, partial_derivative_b, lr=1)

        return best_m, best_b

# -------------------------------------------------------------------------------------------------- #
# Adagrand
# w2 <- w1 - ada1/delta1 * g1
import math 

def adagrand(der_fun, old_der_val, lr, n, old_m) -> float:
   if old_m <= 0.001:
      return old_m
   delta = math.sqrt((der_fun(old_m) ** 2 + (old_der_val) ** 2))
   new_m = old_m - ((lr / delta) * der_fun(old_m))
   n += 1
   return adagrand(der_fun, der_fun(old_m), lr, n, new_m)

m, b = np.polyfit(X, Y, 1)

best_m_from_ada = adagrand(
   partial_derivative_m,
   partial_derivative_m(m),
   lr = 1,
   n = 1,
   old_m = m
)
print("best_m_from_adagrand is ->", best_m_from_ada)
