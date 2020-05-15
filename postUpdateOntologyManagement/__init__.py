import logging
import azure.functions as func
from __app__.shared_code import settings as config
from __app__.shared_code import helper
import json
from datetime import datetime
import pandas as pd

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python update ontology management function processed a request.')
        result=[]
        req_body = req.get_json()
        found_data = update_ontology_value(req_body)
        result = json.dumps(found_data)
    except Exception as e:
        pass
    return func.HttpResponse(result,mimetype="application/json")

def update_ontology_value(update_data):
    try:
        if "ontology_Id" in update_data:
            status=edit_ontology_value(update_data) 
            # pass
        else:
            status=add_ontology_value(update_data)  
        return status
    except Exception as e:
        pass

def add_ontology_value(add_data):
    try:
        current_date=datetime.now()
        conn=helper.SQL_connection()
        cursor=conn.cursor()
        ontology_value=add_data.get('ontologySynonyms',"").replace("'","''")
        inser_value=f"'{add_data.get('synonymsProductName','')}','{add_data.get('synonymsProductType','')}','{ontology_value}','{add_data.get('synonymsCreatedBy','')}','{str(current_date)[:-3]}','{str(current_date)[:-3]}','NULL'" 
        insert_query=f"insert into [momentive].[ontology] values ({inser_value})"
        cursor.execute(insert_query)
        # return "added"
    except Exception as e:
        conn.rollback()
        return "Cannot be added due to some issue"
    else:
        try:
            conn.commit()
            #finding ID from solr 
            query=f'-ID:\-'
            sql_id=0
            ontolgy_result,ontolgy_df=helper.get_data_from_core(config.solr_ontology,query)
            if "ID" in ontolgy_df.columns:
                ontolgy_df=ontolgy_df.replace({"-":"0"})
                ontolgy_df["ID"]= ontolgy_df["ID"].apply(pd.to_numeric)
                list_of_id=list(ontolgy_df["ID"].unique())
            sql_id=max(list_of_id)
            doc={"ONTOLOGY_KEY":add_data.get("synonymsProductName",""),
            "ID":str(sql_id+1), 
            "KEY_TYPE":add_data.get("synonymsProductType",""),
            "ONTOLOGY_VALUE":add_data.get("ontologySynonyms",""),
            "CREATED_BY":add_data.get('synonymsCreatedBy',''),
            "CREATED_DATE":str(current_date)[:-3],
            "UPDATED_DATE":str(current_date)[:-3],
            "PROCESSED_FLAG":""}
            config.solr_ontology.add([doc])
        except Exception as e:
            return "Will be added shortly"
    return "Added sucessfully"
    # sql_status=update_sql_db()
    # solr_status=update_solr_db()

def edit_ontology_value(update_data):
    try:
        current_date=datetime.now()
        conn=helper.SQL_connection()
        cursor=conn.cursor()
        update_value=f"ONTOLOGY_KEY = '{update_data.get('synonymsProductName','')}',KEY_TYPE='{update_data.get('synonymsProductType','')}',ONTOLOGY_VALUE='{update_data.get('ontologySynonyms','')}',UPDATED_DATE='{str(current_date)[:-3]}'"
        update_query=f"update [momentive].[ontology] set {update_value} where id='{update_data.get('ontology_Id','-')}'"
        cursor.execute(update_query)
        conn.commit()
        # return "added"
    except Exception as e:
        return "Cannot be updated"
    else:
        try:
            doc={
            "solr_id":update_data.get("solr_Id","-"),
            "ONTOLOGY_KEY":update_data.get("synonymsProductName",""),
            "KEY_TYPE":update_data.get("synonymsProductType",""),
            "ONTOLOGY_VALUE":update_data.get("ontologySynonyms",""),
            "UPDATED_DATE":(str(current_date)[:-3])
            }
            config.solr_ontology.add([doc],fieldUpdates={"ONTOLOGY_KEY":"set","KEY_TYPE":"set","ONTOLOGY_VALUE":"set","UPDATED_DATE":"set"})
        except Exception as e:
            return "Will be updated shortly"
    return "Updated sucessfully"

def delete_ontology_value(delete_data):
    try:
        conn=helper.SQL_connection()
        cursor=conn.cursor()
        delete_query=f"update [momentive].[ontology] set deleteflag=True where id={delete_data.get('ontologyId','-')}"
        cursor.execute(delete_query)
        conn.commit()
    except Exception as e:
        pass
    else:
        try:
            config.solr_ontology.delete(solr_id=delete_data.get('solr_Id','-'))
        except Exception as e:
            return "Will be deleted shortly"
    return "Deleted sucessfully"