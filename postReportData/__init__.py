import logging
import json
import azure.functions as func
import os 
import pysolr
from __app__.shared_code import settings as config
from __app__.shared_code import helper
solr_document_variant=config.solr_document_variant
get_data_from_core=helper.get_data_from_core
construct_common_level_json=helper.construct_common_level_json

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postReportData function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_report_data_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def get_report_data_details(req_body):
    try:
        all_details_json,spec_list,material_list=construct_common_level_json(req_body) 
        report_list=[]
        spec_list_query=(config.or_delimiter).join(spec_list)
        params={"fl":config.report_column_str}
        core=config.solr_document_variant
        query=f'SUBID:({spec_list_query})'
        result,result_df=get_data_from_core(core,query,params)
        for data in result:
            try:
                if data.get("LANGU").strip()=="E":
                    date_parse=data.get("RELON")
                    date_parse=date_parse.strip()
                    if len(date_parse)==8:
                        date_format=date_parse[0:4]+"-"+date_parse[4:6]+"-"+date_parse[6:8]
                    else:
                        date_format=date_parse
                    specid=data.get("SUBID")
                    namprod=all_details_json.get(specid).get("namprod")
                    material=all_details_json.get(specid).get("material_number")
                    report_json={
                        "category":str(data.get("REPTY")).strip(),
                        "generation_Variant":str(data.get("RGVID")).strip(),
                        "language":str(data.get("LANGU")).strip(),
                        "version":str(data.get("VERSN")).strip(),
                        "released_on":date_format,             
                        "spec_id":specid+(config.hypen_delimiter)+(config.comma_delimiter).join(namprod),
                        "material_details":(config.comma_delimiter).join(material),
                        "status":str(data.get("STATS")).strip(),
                    }
                    report_list.append(report_json)
            except Exception as e:
                print(e)
        return report_list
    except Exception as e:
        print(e)
        return []
