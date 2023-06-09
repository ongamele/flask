from sklearn.gaussian_process import GaussianProcessRegressor
from sklearn.neighbors import KNeighborsRegressor
from sklearn.neural_network import MLPRegressor
from sklearn.tree import DecisionTreeRegressor
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import Ridge
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score


class AutoAI:
    def __init__(self,):
        self.pipelines = None
        pass

    def model_selection(self, X, y):
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2)

        regressors = [
            DecisionTreeRegressor(random_state=0),
            ExtraTreesRegressor(random_state=0),
            KNeighborsRegressor(n_neighbors=2),
            GaussianProcessRegressor(random_state=0),
            MLPRegressor(max_iter=500, random_state=0),
            LinearRegression(),
            RandomForestRegressor(max_depth=2, random_state=0),
            Ridge(random_state=0),
        ]
        self.pipelines = {'score': -99999, 'model': ''}
        for _, regressor in enumerate(regressors):
            regressor.fit(X_train, y_train)
            pred = regressor.predict(X_test)
            score = r2_score(y_test, pred, multioutput='variance_weighted')
            if score > self.pipelines['score']:
                self.pipelines = {'score': score, 'model': _}

        return regressors[self.pipelines['model']]


