import logging
import azure.functions as func
from __app__.shared_code import settings as config
from __app__.shared_code import helper
solr_unstructure_data=config.solr_unstructure_data
import json
import os 
def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postProductCompliance function processing a request.')
        result=[]
        # print(f'AzureBlobStoragePath{os.environ.get("AzureBlobStoragePath")}')
        logging.info(f'{os.environ}')
        # logging.info(f'AzureBlobStorageSasToken{os.environ.get("AzureBlobStorageSasToken")}')
        
        # found_data = get_all_documents()
        # result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

# def get_all_documents():
    # try:
    #     params={"fl":"DATA_EXTRACT,PRODUCT,CATEGORY,PRODUCT_TYPE"}
    #     query=f'IS_RELEVANT:1'
    #     result_json,result_df=helper.get_data_from_core(solr_unstructure_data,query,params)
        
    #     return result_json
    # except Exception as e:
    #     pass

