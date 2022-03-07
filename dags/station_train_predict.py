
def station_train_predict_func(historical_data:list, 
                               historical_column_names:list, 
                               target_column:str,
                               cutpoint: int, 
                               max_epochs: int, 
                               forecast_data:list,
                               forecast_column_names:list,
                               lag_values:list) -> str:
    
    from torch import tensor
    import pandas as pd
    from pytorch_tabnet.tab_model import TabNetRegressor
    from datetime import timedelta
    import numpy as np
    
    feature_columns = historical_column_names.copy()
    feature_columns.remove('DATE')
    feature_columns.remove('STATION_ID')
    feature_columns.remove(target_column)
    
    df = pd.DataFrame(historical_data, columns = historical_column_names)
    #df['DATE']=pd.to_datetime(df['DATE']).dt.date
    
    y_valid = df[target_column][-cutpoint:].values.reshape(-1, 1)
    X_valid = df[feature_columns][-cutpoint:].values
    y_train = df[target_column][:-cutpoint].values.reshape(-1, 1)
    X_train = df[feature_columns][:-cutpoint].values
    
    batch_df = pd.DataFrame(range(2,65,2), columns=['batch_size'])
    batch_df['batch_remainder'] = len(X_train)%batch_df['batch_size']
    optimal_batch_size=int(batch_df['batch_size'].where(batch_df['batch_remainder']==batch_df['batch_remainder'].min()).max())
    
    model = TabNetRegressor()

    model.fit(
        X_train, y_train,
        eval_set=[(X_valid, y_valid)],
        max_epochs=max_epochs,
        patience=100,
        batch_size=optimal_batch_size, 
        virtual_batch_size=optimal_batch_size/2,
        num_workers=0,
        drop_last=True)
    
    df['PRED'] = model.predict(tensor(df[feature_columns].values))

    #Now make the multi-step forecast
    if len(lag_values) > 0:
        forecast_df = pd.DataFrame(forecast_data, columns = forecast_column_names)
        
        for step in range(len(forecast_df)):
            station_id = df['STATION_ID'][-1:].values[0]
            future_date = (pd.to_datetime(df.iloc[-1]['DATE'])+timedelta(days=1)).strftime('%Y-%m-%d')
            lags=[df.shift(lag-1).iloc[-1]['COUNT'] for lag in lag_values]
            forecast=forecast_df.loc[forecast_df['DATE']==future_date]
            forecast=forecast.drop(labels='DATE', axis=1).values.tolist()[0]
            features=[*lags, *forecast]
            pred=round(model.predict(np.array([features]))[0][0])
            row=[future_date, station_id, pred, *features, pred]
            df.loc[len(df)]=row
    
    explain_df = pd.DataFrame(model.explain(df[feature_columns].astype(float).values)[0], 
                         columns = feature_columns).add_prefix('EXPL_').round(2)
    df = pd.concat([df.set_index('DATE').reset_index(), explain_df], axis=1)
   
    return [df.values.tolist(), df.columns.tolist()]
