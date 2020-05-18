import logging
import azure.functions as func
from __app__.shared_code import settings as config
from __app__.shared_code import helper
import json
from datetime import datetime
import pandas as pd
import json

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('Python update ontology document function processed a request.')
        result=[]
        req_body = req.get_json()
        found_data = update_ontology_document(req_body)
        result = json.dumps(found_data)
    except Exception as e:
        pass
    return func.HttpResponse(result,mimetype="application/json")

def update_ontology_document(update_data):
    try:
        current_date=datetime.now()
        conn=helper.SQL_connection()
        cursor=conn.cursor()
        skip_field=["ontology_Id","solr_Id","productName","product_Key","data_extract","category"]
        sql_id=update_data.get('ontology_Id','')
        solr_id=update_data.get('solr_Id','')
        product_name=update_data.get('productName','')
        product_type=update_data.get('product_Key','')
        user=update_data.get('updated_By','')
        data_extract_string=update_data.get('data_extract','')
        # category=update_data.get('category','')
        if sql_id !='' and solr_id !='':
            # updated_data_extract_string=get_updated_extract_string(data_extract_string,skip_field,update_data) 
            updated_data_extract_string=get_updated_extract_string(data_extract_string,update_data) 
            spec_id=get_spec_id_for_updated_value(product_name,product_type)
        update_value=f"is_relevant=1, product = '{product_name}',product_type='{product_type}',data_extract='{updated_data_extract_string}',updated='{str(current_date)[:-3]}',spec_id='{spec_id}'"
        # update_query=f"update [momentive].[unstructure_processed_data] set {update_value} where id='{sql_id}'"
        update_query=f"update [momentive].[unstructure_processed_data_latest_may9] set {update_value} where id='{sql_id}'"
        cursor.execute(update_query)
        # return "added"
    except Exception as e:
        conn.rollback()
        return "Cannot be updated due to some issue"
    else:
        try:
            conn.commit()
            #update in change_audit_log table
            audit_status=helper.update_in_change_audit_log(sql_id,"Ontology Document",user,"update",str(current_date)[:-3])
            doc={
            "solr_id":update_data.get("solr_Id","-"),
            "PRODUCT":product_name,
            "PRODUCT_TYPE":product_type,
            "DATA_EXTRACT":updated_data_extract_string,
            "UPDATED":(str(current_date)[:-3]),
            "IS_RELEVANT":"1",
            "SPEC_ID":spec_id
            }
            if audit_status=="updated in change audit log successfully":
                # pass
                config.solr_ontology.add([doc],fieldUpdates={"PRODUCT":"set","PRODUCT_TYPE":"set","DATA_EXTRACT":"set","UPDATED":"set","IS_RELEVANT":"set","SPEC_ID":"set"})
        except Exception as e:
            return "Will be updated shortly"
    return "Updated sucessfully"

def get_updated_extract_string(data_extract_string,update_data):
    try:
        # object_data=json.loads(data_extract_string)
        object_data=data_extract_string
        extract_field=update_data["Extract_Field"]
        for item in extract_field:
            if item in object_data:
                object_data[item]=extract_field.get(item)
        dump_data=json.dumps(object_data)
        return dump_data
    except Exception as e:
        return ''

def get_spec_id_for_updated_value(product_name,product_type):
    try:
        product_query=helper.replace_character_for_querying([product_name])
        if product_type=="NAMPROD":
            query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT1:({product_query}) && -TEXT6:X'
        elif product_type=="BDT":
            query=f'TYPE:MATNBR && TEXT3:({product_query}) && -TEXT6:X'
        values,df=helper.get_data_from_core(config.solr_product,query)
        if "TEXT2" in df.columns:
            spec_list=list(df["TEXT2"].unique())
            spec_str=";".join(spec_list)
        return spec_str
    except Exception as e:
        return ''
