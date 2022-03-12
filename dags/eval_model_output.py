
def eval_model_output_func(input_data: str, 
                           y_true_name: str, 
                           y_score_name: str) -> str:
    import pandas as pd
    import ast
    from rexmex import RatingMetricSet, ScoreCard
        
    metric_set = RatingMetricSet()
    score_card = ScoreCard(metric_set)
    
    input_data = ast.literal_eval(input_data)

    df = pd.DataFrame(input_data[0], columns=input_data[2])[[y_true_name, y_score_name]]

    df.rename(columns={y_true_name: 'y_true', y_score_name:'y_score'}, inplace=True)
    
    df = score_card.generate_report(df).reset_index()
    
    return [df.values.tolist(), df.columns.tolist()]
