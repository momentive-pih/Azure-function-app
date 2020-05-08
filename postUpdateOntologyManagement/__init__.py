import logging
import azure.functions as func
from __app__.shared_code import settings as config
from __app__.shared_code import helper
import json
from datetime import datetime

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
        if "ID" in update_data:
            pass
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
        inser_value=f"'{add_data.get('synonymsProductName','')}','{add_data.get('synonymsProductType','')}','{add_data.get('ontologySynonyms','')}','{add_data.get('synonymsCreatedBy','')}','{str(current_date)[:-3]}','{str(current_date)[:-3]}','NULL'"
        insert_query=f"insert into [momentive].[ontology] values ({inser_value})"
        cursor.execute(insert_query)
        conn.commit()
        # return "added"
    except Exception as e:
        pass
    else:
        doc={"ONTOLOGY_KEY":add_data.get("synonymsProductName",""),
        "KEY_TYPE":add_data.get("synonymsProductType",""),
        "ONTOLOGY_VALUE":add_data.get("ontologySynonyms",""),
        "CREATED_BY":add_data.get("ontologySynonyms",""),
        "CREATED_DATE":str(current_date)[:-3],
        "UPDATED_DATE":str(current_date)[:-3],
        "PROCESSED_FLAG":""}
        config.solr_ontology.add([doc])
    return "Updated sucessfully"
    # sql_status=update_sql_db()
    # solr_status=update_solr_db()

