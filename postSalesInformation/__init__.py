import logging
import json
import azure.functions as func
import os 
import pysolr
from __app__.shared_code import settings as config
from __app__.shared_code import helper
solr_unstructure_data=config.solr_unstructure_data
# solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
# product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
# solr_product_column = ",".join(product_column)

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postSalesInformation function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_sales_data_details(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def spec_constructor(req_body):
    try:
        last_specid=''
        namlist=[]
        speclist_data=[]
        spec_body=req_body.get("Spec_id")
        for item in spec_body:           
            spec_details=item.get("name").split(" | ")
            spec_id=spec_details[0]
            namprod=spec_details[1]
            if (last_specid!=spec_id) and last_specid!='':
                namstr=", ".join(namlist)
                speclist_data.append([last_specid,namstr])
                namlist=[]
                namlist.append(namprod)
            else:
                namlist.append(namprod)             
            last_specid=spec_id
        namstr=", ".join(namlist)
        speclist_data.append([last_specid,namstr])
        return speclist_data
    except Exception as e:
        return speclist_data


def get_material_details_on_selected_spec(product_rspec,params):
    try:
        query=f'TYPE:MATNBR && TEXT2:{product_rspec}'
        matinfo=solr_product.search(query,**params)
        matstr=[]
        bdt_list=[]
        material_list=[]
        material_details=[]
        for i in list(matinfo):
            bdt=str(i.get("TEXT3")).strip()
            bdt_list.append(bdt)
            matnumber=str(i.get("TEXT1"))
            material_list.append(matnumber)
            desc=str(i.get("TEXT4"))
            matjson={
                        "bdt":bdt,
                        "material_number":matnumber,
                        "description":desc,
                    }
            if bdt:
                bstr=bdt+" - "+matnumber+" - "+desc
                matstr.append(bstr)
            material_details.append(matjson)
        material_list=list(set(material_list))
        bdt_list=list(set(bdt_list))
        return material_list,bdt_list,matstr,material_details
    except Exception as e:
        return [],[],[],[]

def get_sales_data_details(req_body):
    try:
        category=["SAP-BW"]
        material_level_data=req_body.get("Mat_Level")
        all_details_json,spec_list,material_list=helper.construct_common_level_json(req_body)
        sale_info_query=helper.unstructure_template(all_details_json,category)
        params={"fl":config.unstructure_column_str}
        result,result_df=helper.get_data_from_core(solr_unstructure_data,sale_info_query,params)
        # speclist_data=spec_constructor(req_body)
        sales_list=[]
        # params={"rows":2147483647,"fl":"TEXT1,TEXT2,TEXT3,TEXT4"}
        # for spec,namprod in speclist_data:
        #     material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(spec,params)
        #     for matid in material_details:
        #         sales_column_str="DATA_EXTRACT"
        #         material_number=matid.get("material_number")
        #         basic_data=matid.get("bdt")
        #         material_description=matid.get("description")
        #         query=f'CATEGORY:SAP-BW && IS_RELEVANT:1 && PRODUCT:{material_number}'
        #         params={"rows":2147483647,"fl":sales_column_str}
        #         result = list(solr_unstructure_data.search(query,**params))    
        sales_kg=0
        sales_org=[]
        region=[]
        material_flag=''
        for mat_id in material_list:
            for data in result: 
                material_number=data.get("PRODUCT")
                if material_number==mat_id:
                    material_flag='s'
                    result_spec=data.get("SPEC_ID")  
                    spec_id=helper.finding_spec_details(spec_list,result_spec)
                    data_extract=json.loads(data.get("DATA_EXTRACT"))
                    sales_org.append(data_extract.get("Sales Organization"))
                    sales_kg=sales_kg+int(data_extract.get("SALES KG"))
                    region.append(data_extract.get("Sold-to Customer Country"))
                    sales_org=list(set(sales_org))
                    region=list(set(region))
                    sales_org=", ".join(sales_org)
                    region=",".join(region)
                    material_number=data.get("PRODUCT")
            if material_flag=='s':
                desc=[]
                bdt=[]       
                for item in material_level_data:
                    if item.get("material_Number")==mat_id:
                        desc.append(item.get("description"))
                        bdt.append(item.get("bdt"))
                sales_json={
                    "material_number":mat_id,
                    "material_description":(config.comma_delimiter).join(list(set(desc))),
                    "basic_data":(config.comma_delimiter).join(list(set(bdt))),
                    "sales_Org":sales_org,
                    "past_Sales":str(sales_kg)+" Kg",
                    "spec_id":spec_id,
                    "region_sold":region
                    }
                sales_list.append(sales_json) 
                sales_json={}
                material_flag=''
        result_data={"saleDataProducts":sales_list}
        return [result_data]
    except Exception as e:
        print(e)
        return []
