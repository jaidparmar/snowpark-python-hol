
def station_train_predict_func(input_data: list, 
                               input_columns: list, 
                               target_column: str,
                               max_epochs: int) -> str:

    import pandas as pd
    df = pd.DataFrame(input_data, columns = input_columns)
    
    #Due to annual seasonality we need at least one year of data for training 
    #and a second year of data for validation
    if len(df) < 365*2:
        df['PRED'] = 'NULL'
    else:
        feature_columns = input_columns.copy()
        feature_columns.remove('DATE')
        feature_columns.remove(target_column)

        from torch import tensor
        from pytorch_tabnet.tab_model import TabNetRegressor

        model = TabNetRegressor()

        #cutpoint = round(len(df)*(train_valid_split/100))
        cutpoint = 365

        ##NOTE: in order to do train/valid split on time-based portion the input data must be sorted by date    
        df['DATE'] = pd.to_datetime(df['DATE'])
        df = df.sort_values(by='DATE', ascending=True)

        y_valid = df[target_column][-cutpoint:].values.reshape(-1, 1)
        X_valid = df[feature_columns][-cutpoint:].values
        y_train = df[target_column][:-cutpoint].values.reshape(-1, 1)
        X_train = df[feature_columns][:-cutpoint].values

        model.fit(
            X_train, y_train,
            eval_set=[(X_valid, y_valid)],
            max_epochs=max_epochs,
            patience=100,
            batch_size=1024, 
            virtual_batch_size=128,
            num_workers=0,
            drop_last=False)

        df['PRED'] = model.predict(tensor(df[feature_columns].round(2).values))
        df['DATE'] = df['DATE'].dt.strftime('%Y-%m-%d')
        df = pd.concat([df, pd.DataFrame(model.explain(df[feature_columns].values)[0], 
                               columns = feature_columns).add_prefix('EXPL_').round(2)], axis=1)
    
    return [df.values.tolist(), df.columns.tolist()]
