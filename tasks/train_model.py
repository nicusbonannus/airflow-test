import joblib
import pandas as pd
import xgboost as xgb


def train_model(**kwargs):
    ti = kwargs['ti']
    df = pd.read_json(ti.xcom_pull(task_ids='clean_data', key='clean_data'))
    # Train model
    X_train = df[
        ['Duration', 'Total_Stops', 'Source', 'Destination', 'Additional_Info', 'Dep_Time', 'Arrival_Time', 'Day',
         'Month', 'Year']]
    y_train = df['Price']

    params = {
        'n_estimators': 200,
        'learning_rate': 0.5,
        'max_depth': 6,
        'min_child_weight': 10,
        'gamma': 10,
        'subsample': 1,
        'colsample_bytree': 1,
        'reg_alpha': 10,
        'reg_lambda': 10
    }
    model = xgb.XGBRegressor(**params)

    model.fit(X_train, y_train)

    # save model
    with open('models/price_predictor.pkl', 'wb') as f:
        joblib.dump(model, f)