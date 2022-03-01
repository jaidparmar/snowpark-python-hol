
def incremental_elt(session, 
                    state_dict:dict, 
                    files_to_ingest:list, 
                    download_role_ARN='',
                    download_base_url='') -> str:
    
    import dags.elt as ELT
    
    load_stage_name=state_dict['load_stage_name']
    load_table_name=state_dict['load_table_name']
    trips_table_name=state_dict['trips_table_name']
    
    if download_role_ARN and download_base_url:
        print("Skipping extract.  Using provided bucket.")
        sql_cmd = 'CREATE OR REPLACE TEMPORARY STAGE '+str(load_stage_name)+\
                  ' url='+str(download_base_url)+\
                  " credentials=(aws_role='" + str(download_role_ARN)+"')"
        session.sql(sql_cmd).collect()
        
        files_to_load = [file.replace('.zip','.gz') for file in files_to_ingest]
    
    else:
        print("Extracting files from public location.")
        download_base_url=state_dict['download_base_url']
        _ = session.sql('CREATE OR REPLACE TEMPORARY STAGE '+str(load_stage_name)).collect()        
        load_stage_name, files_to_load = ELT.extract_trips_to_stage(session=session, 
                                                                    files_to_download=files_to_ingest, 
                                                                    download_base_url=download_base_url, 
                                                                    load_stage_name=load_stage_name)
        

    print("Loading files to raw.")
    stage_table_names = ELT.load_trips_to_raw(session=session, 
                                              files_to_load=files_to_load, 
                                              load_stage_name=load_stage_name, 
                                              load_table_name=load_table_name)
    
    print("Transforming records to trips table.")
    trips_table_name = ELT.transform_trips(session=session, 
                                           stage_table_names=stage_table_names, 
                                           trips_table_name=trips_table_name)
    return trips_table_name

def bulk_elt(session, 
             state_dict:dict,
             download_role_ARN='', 
             download_base_url=''
            ) -> str:
    
    import dags.elt as ELT
    from dags.ingest import incremental_elt
    
    import pandas as pd
    from datetime import datetime

    #Create a list of filenames to download based on date range
    #For files like 201306-citibike-tripdata.zip
    date_range1 = pd.period_range(start=datetime.strptime("201306", "%Y%m"), 
                                 end=datetime.strptime("201612", "%Y%m"), 
                                 freq='M').strftime("%Y%m")
    file_name_end1 = '-citibike-tripdata.zip'
    files_to_extract = [date+file_name_end1 for date in date_range1.to_list()]

    #For files like 201701-citibike-tripdata.csv.zip
    date_range2 = pd.period_range(start=datetime.strptime("201701", "%Y%m"), 
                                 end=datetime.strptime("202002", "%Y%m"), 
                                 freq='M').strftime("%Y%m")
    file_name_end2 = '-citibike-tripdata.csv.zip'
    files_to_extract = files_to_extract + [date+file_name_end2 for date in date_range2.to_list()]

    if download_role_ARN and download_base_url:
        trips_table_name = incremental_elt(session=session, 
                                           state_dict=state_dict, 
                                           files_to_ingest=files_to_extract, 
                                           download_role_ARN=download_role_ARN,
                                          download_base_url=download_base_url)
    else:
        trips_table_name = incremental_elt(session=session, 
                                           state_dict=state_dict, 
                                           files_to_ingest=files_to_extract)
    
    return trips_table_name
