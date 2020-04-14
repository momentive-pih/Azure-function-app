import logging
import json
import azure.functions as func
import pandas as pd
import os 
import pysolr
from __app__.shared_code import settings as config
from __app__.shared_code import helper
# solr_url_config="https://172.23.2.10:8983/solr"
# solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
# solr_notification_status=pysolr.Solr(solr_url_config+'/sap_notification_status/', timeout=10,verify=False)
# solr_unstructure_data=pysolr.Solr(solr_url_config+'/unstructure_processed_data/', timeout=10,verify=False)
# solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
# solr_ghs_labeling_list_data=pysolr.Solr(solr_url_config+'/sap_ghs_labeling_list_data/', timeout=10,verify=False)
# solr_ontology=pysolr.Solr(solr_url_config+'/ontology/',timeout=10,verify=False)
# solr_substance_identifier=pysolr.Solr(solr_url_config+'/sap_substance_identifier/',timeout=10,verify=False)
# solr_phrase_translation=pysolr.Solr(solr_url_config+'/sap_phrase_translation/',timeout=10,verify=False)
# solr_inci_name=pysolr.Solr(solr_url_config+'/inci_name_prod/',timeout=10,verify=False)
# solr_std_composition=pysolr.Solr(solr_url_config+'/sap_standard_composition/',timeout=10,verify=False)
# solr_hundrd_composition=pysolr.Solr(solr_url_config+'/sap_hundrd_percent_composition/',timeout=10,verify=False)
# solr_legal_composition=pysolr.Solr(solr_url_config+'/sap_legal_composition/',timeout=10,verify=False)
# solr_substance_volume_tracking=pysolr.Solr(solr_url_config+'/sap_substance_volume_tracking/',timeout=10,verify=False)
# product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
# solr_product_column = ",".join(product_column)
# file_access_path="https://devstorpih001.blob.core.windows.net/"
# ghs_image_path="https://devstorpih001.blob.core.windows.net/momentive-sources-pih/ghs-images-pih/"
# sas_token=r"?sv=2019-02-02&ss=b&srt=sco&sp=rl&se=2020-05-29T20:19:29Z&st=2020-04-02T12:19:29Z&spr=https&sig=aodIg0rDPVsNEJY7d8AerhD79%2FfBO9LZGJdx2j9tsCM%3D"

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info('postToxicology function processing a request.')
        result=[]
        req_body = req.get_json()
        found_data = get_product_attributes(req_body[0])
        result = json.dumps(found_data)
    except Exception as e:
        logging.error(str(e))
    return func.HttpResponse(result,mimetype="application/json")

def spec_constructor(req_body):
    try:
        last_specid=''
        namlist=[]
        speclist_json={}
        total_spec=[]
        spec_body=req_body.get("Spec_id",[])
        for item in spec_body:           
            spec_details=item.get("name").split(config.pipe_delimitter)
            if len(spec_details)>0:
                spec_id=spec_details[0]
                namprod=str(spec_details[1]).strip()
            if spec_id!='':
                total_spec.append(spec_id)
            if (last_specid!=spec_id) and last_specid!='':
                namstr=", ".join(namlist)
                speclist_json[last_specid]=namstr
                namlist=[]
                if namprod != "-":
                    namlist.append(namprod)        
            else:
                if namprod != "-":
                    namlist.append(namprod)    
            last_specid=spec_id
        namstr=", ".join(namlist)
        speclist_json[last_specid]=namstr
        return speclist_json,list(set(total_spec))
    except Exception as e:
        return speclist_json,list(set(total_spec))

def get_product_attributes(req_body):
    try:  
        product_attributes_result=[]
        json_list=[]
        sub_category=req_body.get("Category_details").get("Subcategory")    
        validity=req_body.get("Category_details").get("validity")
        if sub_category=="Basic Information":
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            idtxt=[]
            #finding Relables
            spec_join=(config.or_delimiter).join(spec_list)
            spec_query=f'SUBID:({spec_join})'
            params={"fl":config.relable_column_str}
            result,result_df=helper.get_data_from_core(config.solr_substance_identifier,spec_query,params) 
            if len(result_df.columns)!=len(config.relable_column):
                dummy=pd.DataFrame([],columns=config.relable_column)
                result_df=pd.concat([result_df,dummy])
            result_df=result_df.fillna("-")  
            result_df=result_df.replace({"NULL":"-"})
            for item in all_details_json:
                json_make={}
                json_make["spec_id"]=item
                json_make["product_Identification"]=(config.comma_delimiter).join(all_details_json.get(item).get("namprod",[]))   
                idtxt_df=result_df[(result_df["IDCAT"]=="NAM") & (result_df["IDTYP"]=="PROD_RLBL") & (result_df["LANGU"].isin(["E","","-"])) & (result_df["SUBID"]==item)]
                idtxt=list(idtxt_df["IDTXT"].unique())
                if len(idtxt)>0:
                    json_make["relabels"]=(config.comma_delimiter).join(idtxt)
                else:
                    json_make["relabels"]="-"
                json_list.append(json_make)
            product_attributes_result.append({"basic_details":json_list})
            #product Application
            json_list=[]
            category=["Prod-App"]
            prod_query=helper.unstructure_template(all_details_json,category)
            params={"fl":config.unstructure_column_str}
            unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,prod_query,params)        
            if len(unstructure_values)>0:
                try:
                    for data in unstructure_values:
                        json_make={}
                        product=data.get("PRODUCT",config.hypen_delimiter)
                        product_type=data.get("PRODUCT_TYPE",config.hypen_delimiter)
                        datastr=json.loads(data.get("DATA_EXTRACT",{}))
                        result_spec=data.get("SPEC_ID")
                        spec_id=helper.finding_spec_details(spec_list,result_spec)
                        path=datastr.get("image_path",config.hypen_delimiter)
                        file_split=path.split("/")
                        file_source=''
                        for source in config.file_sources:
                            if source in file_split:
                                file_source=source
                                break
                        filename=datastr.get("file_name",config.hypen_delimiter)
                        json_make["filename"]=filename
                        json_make["file_source"]=file_source
                        json_make["product"]=product
                        json_make["product_Type"]=product_type
                        json_make["prod_App"]=config.blob_file_path+path.replace("/dbfs/mnt/","")+config.sas_token
                        json_make["spec_Id"]=spec_id
                        json_list.append(json_make)          
                except Exception as e:
                    pass
            product_attributes_result.append({"product_Application":json_list})     
        elif sub_category=="GHS Labeling":
            spec_json,spec_list=spec_constructor(req_body)
            spec_join=(config.or_delimiter).join(spec_list)
            spec_query=f'SUBID:({spec_join})'
            ghs_values,ghs_df=helper.get_data_from_core(config.solr_ghs_labeling_list_data,spec_query)
            total_phrky=[]
            if len(ghs_values)>0:
                for key_column in config.ghs_label:
                    if key_column in list(ghs_df.columns):
                        phrase_key=list(ghs_df[key_column].unique())
                        phrase_split=";".join(phrase_key)
                        total_phrky+=phrase_split.split(";")    
                #finding phrase text
                phrase_key_query=(config.or_delimiter).join(total_phrky)
                query=f'PHRKY:({phrase_key_query})'
                params={"fl":config.phrase_column_str}
                key_value,key_value_df=helper.get_data_from_core(config.solr_phrase_translation,query,params)          
                for data in ghs_values:
                    json_make={}
                    specid=data.get("SUBID","")      
                    spec_nam_str=specid+(config.hypen_delimiter)+spec_json.get(specid,"")
                    json_make["spec_Id"]=spec_nam_str
                    json_make["usage"]=str(data.get("ZUSAGE",config.hypen_delimiter).strip())
                    json_make["regulatory_Basis"]=helper.finding_phrase_text(key_value_df,str(data.get("REBAS","")).strip())
                    json_make["signal_Word"]=helper.finding_phrase_text(key_value_df,str(data.get("SIGWD","")).strip())
                    json_make["hazard_Statements"]=helper.finding_phrase_text(key_value_df,str(data.get("HAZST","")).strip())
                    json_make["prec_Statements"]=helper.finding_phrase_text(key_value_df,str(data.get("PRSTG","")).strip())
                    json_make["additional_Information_remarks"]=(helper.finding_phrase_text(key_value_df,str(data.get("ADDIN","")).strip()))+(config.hypen_delimiter)+(helper.finding_phrase_text(key_value_df,str(data.get("REMAR","")).strip()))
                    #symbols
                    symbols=[]
                    path_list=[]
                    symbol_value=str(data.get("SYMBL","")).strip()
                    key_list=symbol_value.split(';')
                    if len(key_list)>0 and ("PHRKY" in list(key_value_df.columns)) and ("GRAPH" in list(key_value_df.columns)):
                        text_df=key_value_df[key_value_df["PHRKY"].isin(key_list)]
                        path_list=list(text_df["GRAPH"].unique())
                    if len(path_list)>0:
                        for file in path_list:
                            path=(config.ghs_image_path)+file+(config.sas_token)
                            symbols.append({"name":path})
                    json_make["symbols"]=symbols
                    json_list.append(json_make)
            product_attributes_result.append({"ghs_Labeling":json_list})  
        elif sub_category in ["Structures and Formulas","Flow Diagrams"]:
            chem_structure=[]
            molecular_formula=[]
            molecular_weight=[]
            man_flow_dg=[]
            synthesis_dg=[]
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            if sub_category=="Structures and Formulas":
                un_category=config.structure_category
            else:
                un_category=["man_flow_diagram","syn_flow_diagram"]
            query=helper.unstructure_template(all_details_json,un_category)
            params={"fl":config.unstructure_column_str}
            unstructure_values,unstructure_df=helper.get_data_from_core(config.solr_unstructure_data,query,params)        
            if len(unstructure_values)>0:
                for item in unstructure_values:
                    try:
                        json_make={}
                        datastr={}
                        category=item.get("CATEGORY",config.hypen_delimiter)
                        datastr=json.loads(item.get("DATA_EXTRACT",{}))
                        result_spec=item.get("SPEC_ID")
                        product=item.get("PRODUCT",config.hypen_delimiter)
                        product_type=item.get("PRODUCT_TYPE",config.hypen_delimiter)
                        spec_id=helper.finding_spec_details(spec_list,result_spec) 
                        path=datastr.get("file_path",config.hypen_delimiter)
                        file_split=path.split("/")
                        file_source=''
                        for source in config.file_sources:
                            if source in file_split:
                                file_source=source
                                break
                        json_make["spec_Id"]=spec_id
                        json_make["file_Source"]=file_source
                        json_make["product_Type"]=product_type
                        json_make["productName"]=product
                        if category=="Chemical Structure":
                            filename=datastr.get("file_path",config.hypen_delimiter).split("/")
                            if len(filename)>0:
                                json_make["fileName"]=filename[-1]
                            else:
                                json_make["fileName"]=config.hypen_delimiter
                            json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)
                            chem_structure.append(json_make)
                        elif category=="molecular formula":
                            path=datastr.get("image_path","-")
                            json_make["fileName"]=datastr.get("file_name",config.hypen_delimiter)
                            json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)  
                            molecular_formula.append(json_make)               
                        elif category=="Molecular-Weight":
                            weight=datastr.get("Molecular Weight",config.hypen_delimiter)
                            # weight=weight.replace("Molecular Weight:","").strip()
                            json_make["moelcular_Weight"]=weight
                            molecular_weight.append(json_make)   
                        elif category=="man_flow_diagram":
                            filename=datastr.get("file_path",config.hypen_delimiter).split("/")
                            if len(filename)>0:
                                json_make["fileName"]=filename[-1]
                            else:
                                json_make["fileName"]=config.hypen_delimiter
                            json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)
                            man_flow_dg.append(json_make)
                            
                        elif category=="syn_flow_diagram":
                            filename=datastr.get("file_path",config.hypen_delimiter).split("/")
                            if len(filename)>0:
                                json_make["fileName"]=filename[-1]
                            else:
                                json_make["fileName"]=config.hypen_delimiter
                            json_make["file_Path"]=(config.blob_file_path)+path.replace("/dbfs/mnt/","")+(config.sas_token)
                            synthesis_dg.append(json_make)
                            json_make={}
                    except Exception as e:
                        pass
            if sub_category=="Structures and Formulas":        
                product_attributes_result.append({"chemical_Structure":chem_structure})
                product_attributes_result.append({"molecular_Formula":molecular_formula})
                product_attributes_result.append({"molecular_Weight":molecular_weight})
            else:
                product_attributes_result.append({"manufacture_Flow":man_flow_dg})
                product_attributes_result.append({"synthesis_Diagram":synthesis_dg})
        elif sub_category=="Composition":
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            idtxt=[]
            #finding Relables
            spec_join=(config.or_delimiter).join(spec_list)
            spec_query=f'SUBID:({spec_join})'
            params={"fl":config.relable_column_str}
            result,result_df=helper.get_data_from_core(config.solr_substance_identifier,spec_query,params) 
            if len(result_df.columns)!=len(config.relable_column):
                dummy=pd.DataFrame([],columns=config.relable_column)
                result_df=pd.concat([result_df,dummy])
            result_df=result_df.fillna("-")  
            result_df=result_df.replace({"NULL":"-"})
            for item in all_details_json:
                try:
                    json_make={}
                    json_make["spec_id"]=item
                    json_make["product_Identification"]=(config.comma_delimiter).join(all_details_json.get(item).get("namprod",[]))   
                    idtxt_df=result_df[(result_df["IDCAT"]=="NAM") & (result_df["IDTYP"]=="PROD_RLBL") & (result_df["LANGU"].isin(["E","","-"])) & (result_df["SUBID"]==item)]
                    idtxt=list(idtxt_df["IDTXT"].unique())
                    if len(idtxt)>0:
                        json_make["relabels"]=(config.comma_delimiter).join(idtxt)
                    else:
                        json_make["relabels"]="-"
                except Exception as e:
                    pass
            #finding inciname
            display_inci_name=[]
            df_inci=result_df[result_df["IDTYP"]=='INCI']
            df_inci.drop_duplicates(inplace=True)
            if "IDTXT" in list(df_inci.columns):
                inci_name=list(df_inci["IDTXT"].unique())
                if len(inci_name)>0:
                    inci_query=helper.replace_character_for_querying(inci_name)
                    query=f'INCINAME:({inci_query}) && SUBID:({spec_join})'
                    inci_values,inci_df=helper.get_data_from_core(config.solr_inci_name,query) 
                    inci_df.drop_duplicates(inplace=True)
                    if "INCINAME" in list(inci_df.columns) and "BDTXT" in list(inci_df.columns):
                        bdtxt_df=inci_df[["BDTXT","INCINAME"]]
                        bdtxt_df.drop_duplicates(inplace=True)
                        bdtxt_list=bdtxt_df.values.tolist()
                        for bdtxt,inci in bdtxt_list:
                            temp=bdtxt+(config.pipe_delimitter)+inci
                            display_inci_name.append(temp)                    
            json_make["INCI_name"]=(config.comma_delimiter).join(display_inci_name)
            json_list.append(json_make)
            #finding material level
            materials=[]
            for spec in all_details_json:
                materials+=all_details_json.get(spec).get("material_number",[])
            material_query=(config.or_delimiter).join(materials)
            active_material=[]
            all_material=[]
            if material_query!='':
                query=f'TYPE:MATNBR && TEXT1:({material_query}) && TEXT2:({spec_join}) && -TYPE:SUBIDREL && -TEXT6:X'
                params={"fl":config.solr_product_column}
                mat_values,mat_df=helper.get_data_from_core(config.solr_product,query,params) 
                for item in mat_values:
                    try:
                        json_make={}
                        material_number=item.get("TEXT1",config.hypen_delimiter)
                        description=item.get("TEXT4",config.hypen_delimiter)
                        bdt=item.get("TEXT3",config.hypen_delimiter)
                        if str(item.get("TEXT5")).strip() != 'X':
                            json_make["material_Number"]=material_number
                            json_make["description"]=description
                            json_make["bdt"]=bdt
                            active_material.append(json_make)
                            json_make={}
                        json_make["material_Number"]=material_number
                        json_make["description"]=description
                        json_make["bdt"]=bdt
                        all_material.append(json_make)
                    except Exception as e:
                        pass
            json_make={}
            json_make["product_Level"]=json_list
            json_make["active_material"]=active_material
            json_make["all_material"]=all_material
            product_attributes_result=[json_make]
        elif sub_category in ["Standard, 100 % & INCI Composition","Legal Composition"]:
            all_details_json,spec_list,material_list = helper.construct_common_level_json(req_body)
            cas_list=[]
            spec_list=[]
            for spec in all_details_json:
                spec_list.append(spec)
                cas_list+=all_details_json.get(spec).get("pure_spec_id")
            cas_query=(config.or_delimiter).join(cas_list)
            spec_query=(config.or_delimiter).join(spec_list)
            if validity is None:
                std_usage=[]
                hundrd_usage=[]
                legal_usage=[]
                if cas_query!='' and spec_query!='':
                    query=f'CSUBI:({cas_query}) && SUBID:({spec_query})'
                    # query=f'SUBID:({spec_query})'
                    if sub_category=="Standard, 100 % & INCI Composition":
                        std_values,std_df=helper.get_data_from_core(config.solr_std_composition,query) 
                        # std_df=std_df[std_df["CSUBI"].isin(cas_list)]       
                        if "ZUSAGE" in list(std_df.columns):
                            std_usage=list(std_df["ZUSAGE"].unique())
                        hund_values,hund_df=helper.get_data_from_core(config.solr_hundrd_composition,query)
                        # hund_df=hund_df[hund_df["CSUBI"].isin(cas_list)]
                        if "ZUSAGE" in list(hund_df.columns):
                            hundrd_usage=list(hund_df["ZUSAGE"].unique())           
                        usage_catgory=std_usage+hundrd_usage
                        json_list=[]
                        for i in list(set(usage_catgory)):
                            json_make={}
                            json_make["name"]=i
                            json_list.append(json_make)   
                    elif sub_category=="Legal Composition":
                        json_list=[]
                        legal_values,legal_df=helper.get_data_from_core(config.solr_legal_composition,query)  
                        # legal_df=legal_df[legal_df["CSUBI"].isin(cas_list)] 
                        if "ZUSAGE" in list(legal_df.columns):
                            legal_usage=list(legal_df["ZUSAGE"].unique())
                        for i in list(set(legal_usage)):
                            json_make={}
                            json_make["name"]=i
                            json_list.append(json_make)
                    return json_list         
            if validity is not None:
                zusage_value=helper.replace_character_for_querying([validity])
                query=f'CSUBI:({cas_query}) && ZUSAGE:({zusage_value}) && SUBID:({spec_query})'    
                # query=f'ZUSAGE:({zusage_value}) && SUBID:({spec_query})'                       
                if sub_category=="Standard, 100 % & INCI Composition":
                    std_result=[]
                    hundrd_result=[]
                    inci_result=[]
                    std_values,std_df=helper.get_data_from_core(config.solr_std_composition,query)        
                    hund_values,hund_df=helper.get_data_from_core(config.solr_hundrd_composition,query)
                    cidp_query=helper.unstructure_template(all_details_json,["CIDP"])
                    params={"fl":config.unstructure_column_str}
                    cidp_values,cidp_df=helper.get_data_from_core(config.solr_unstructure_data,cidp_query,params)        
                    for item in req_body.get("CAS_Level"):
                        real_spec_list=item.get("real_Spec_Id")
                        for real in real_spec_list:
                            if spec_query in real:
                                std_flag=''
                                hundrd_flag=''
                                inci_flag=''
                                json_make={} 
                                for std in std_values:
                                    if std.get("CSUBI").strip()==item.get("pure_Spec_Id"):
                                        std_flag='s'
                                        json_make["std_Componant_Type"]=std.get("COMPT","-")
                                        json_make["std_value"]=std.get("CVALU","-")
                                        json_make["std_unit"]=std.get("CUNIT","-")
                                for hundrd in hund_values:
                                    if hundrd.get("CSUBI").strip()==item.get("pure_Spec_Id"):
                                        hundrd_flag='s'
                                        json_make["hundrd_Componant_Type"]=hundrd.get("COMPT","-")
                                        json_make["hundrd_value"]=hundrd.get("CVALU","-")
                                        json_make["hundrd_unit"]=hundrd.get("CUNIT","-")
                                for inci in cidp_values:
                                    data=json.loads(inci.get("DATA_EXTRACT"))
                                    inci_cas_number=data.get("CAS Number ").strip()
                                    if inci_cas_number==item.get("cas_Number"):
                                        inci_flag='s'
                                        json_make["inci_Componant_Type"]="Active"
                                        json_make["inci_value_unit"]=data.get("Target Composition","-")
                                if std_flag =='':
                                    json_make["std_Componant_Type"]='-'
                                    json_make["std_value"]='-'
                                    json_make["std_unit"]="-"
                                if hundrd_flag=='':
                                    json_make["hundrd_Componant_Type"]="-"
                                    json_make["hundrd_value"]="-"
                                    json_make["hundrd_unit"]="-"
                                if inci_flag=='':
                                    json_make["inci_Componant_Type"]="-"
                                    json_make["inci_value_unit"]="-"
                                if std_flag=='s' or hundrd_flag=='s' or inci_flag=='s':
                                    json_make["pure_spec_Id"]=item.get("pure_Spec_Id")
                                    json_make["cas_Number"]=item.get("cas_Number")
                                    json_make["ingredient_Name"]=item.get("chemical_Name")
                                    json_list.append(json_make)
                                break
                    if len(json_list)>0:
                        total_std_value=0
                        total_hundrd_value=0
                        total_inci_value=0
                        for item in json_list:
                            try:
                                if item.get("std_value") !="-":
                                    total_std_value+=float(item.get("std_value"))
                                if item.get("hundrd_value") !="-":
                                    total_hundrd_value+=float(item.get("hundrd_value"))
                                if item.get("inci_value_unit") !="-":
                                    inci_list=[incv for incv in str(item.get("inci_value_unit")) if(incv.isdigit() or incv==".")]
                                    inci_str="".join(inci_list)
                                    total_inci_value+=float(inci_str)
                            except Exception as e:
                                pass
                        json_make={}
                        json_make["pure_spec_Id"]="Total"
                        json_make["cas_Number"]=""
                        json_make["ingredient_Name"]=""
                        json_make["std_Componant_Type"]=""
                        json_make["std_value"]=total_std_value
                        json_make["std_unit"]=""
                        json_make["hundrd_Componant_Type"]=""
                        json_make["hundrd_value"]=total_hundrd_value
                        json_make["hundrd_unit"]=""
                        json_make["inci_Componant_Type"]=""
                        json_make["inci_value_unit"]=total_inci_value
                        json_list.append(json_make)
                    return json_list
                elif sub_category=="Legal Composition":
                    json_list=[]
                    legal_values,legal_df=helper.get_data_from_core(config.solr_legal_composition,query)        
                    legal_df=legal_df[legal_df["CSUBI"].isin(cas_list)]
                    legal_svt_spec=[]
                    legal_comp={}
                    total_legal_value=0
                    for item in req_body.get("CAS_Level"):
                        real_spec_list=item.get("real_Spec_Id")
                        for real in real_spec_list:
                            if spec_query in real:
                                for data in legal_values:
                                    json_make={}
                                    if data.get("CSUBI")==item.get("pure_Spec_Id"):
                                        legal_svt_spec.append(item.get("pure_Spec_Id"))
                                        json_make["pure_spec_Id"]=item.get("pure_Spec_Id")
                                        json_make["cas_Number"]=item.get("cas_Number")
                                        json_make["ingredient_Name"]=item.get("chemical_Name")
                                        json_make["legal_Componant_Type"]=data.get("COMPT","-")
                                        json_make["legal_value"]=data.get("CVALU","-")
                                        if data.get("CVALU","-") !="-":
                                            total_legal_value+=float(data.get("CVALU",0))
                                        json_make["legal_unit"]=data.get("CUNIT","-")
                                        json_list.append(json_make) 
                                break 
                    if len(json_list)>0:
                        json_make={}
                        json_make["pure_spec_Id"]="Total"
                        json_make["cas_Number"]=""
                        json_make["ingredient_Name"]=""
                        json_make["legal_Componant_Type"]=""
                        json_make["legal_value"]=total_legal_value
                        json_make["legal_unit"]=""
                        json_list.append(json_make)
                    legal_comp["legal_composition"]=json_list
                    if validity=='REACH: REG_REACH':
                        json_list=[]
                        json_make={}
                        svt_result=[]
                        if len(legal_svt_spec)>0:
                            subid_list=(config.or_delimiter).join(legal_svt_spec)
                            query=f'SUBID:({subid_list})'
                            svt_result,svt_df=helper.get_data_from_core(config.solr_substance_volume_tracking,query)        
                        presence_id=[]
                        presence_id=list(svt_df["SUBID"].unique())
                        for sub in presence_id:
                            json_make["pure_spec_Id"]=sub
                            svt_total_2018=0
                            svt_total_2019=0
                            svt_total_2020=0
                            for data in svt_result:
                                if sub==data.get("SUBID","-"):
                                    reg_value=data.get("REGLT","-")
                                    reg_year=data.get("QYEAR","-").strip()
                                    if reg_value=="SVT_TE":
                                        if reg_year=="2018":
                                            json_make["SVT_TE_eight"]=data.get("CUMQT","-")
                                        if reg_year=="2019":
                                            json_make["SVT_TE_nine"]=data.get("CUMQT","-")
                                        if  reg_year=="2020":
                                            json_make["SVT_TE_twenty"]=data.get("CUMQT","-")
                                        json_make["amount_limit_SVT_TE"]=data.get("AMTLT","0")
                                    if reg_value=="SVT_AN":
                                        if reg_year=="2018":
                                            json_make["SVT_AN_eight"]=data.get("CUMQT","-")
                                        if reg_year=="2019":
                                            json_make["SVT_AN_nine"]=data.get("CUMQT","-")
                                        if  reg_year=="2020":
                                            json_make["SVT_AN_twenty"]=data.get("CUMQT","-")
                                        json_make["amount_limit_SVT_AN"]=data.get("AMTLT","0")
                                    if reg_value=="SVT_LV":
                                        if reg_year=="2018":
                                            svt_total_2018+=float(data.get("CUMQT","-"))
                                            json_make["SVT_LV_eight"]=svt_total_2018
                                        if reg_year=="2019":
                                            svt_total_2019+=float(data.get("CUMQT","-"))
                                            json_make["SVT_LV_nine"]=svt_total_2019
                                        if  reg_year=="2020":
                                            svt_total_2020+=float(data.get("CUMQT","-"))
                                            json_make["SVT_LV_twenty"]=svt_total_2020
                                        json_make["amount_limit_SVT_LV"]=data.get("AMTLT","0")                   
                            json_list.append(json_make)
                            json_make={}
                        total_svt_te_amt=0
                        total_svt_an_amt=0
                        total_svt_lv_amt=0
                        for item in json_list:
                            total_svt_te_amt=total_svt_te_amt+float(item.get("amount_limit_SVT_TE",0))
                            total_svt_an_amt=total_svt_an_amt+float(item.get("amount_limit_SVT_AN",0))
                            total_svt_lv_amt=total_svt_lv_amt+float(item.get("amount_limit_SVT_LV",0))
                        json_make["pure_spec_Id"]="Total"
                        json_make["SVT_TE_eight"]=""
                        json_make["SVT_TE_nine"]=""
                        json_make["SVT_TE_twenty"]=""
                        json_make["amount_limit_SVT_TE"]=str(total_svt_te_amt)
                        json_make["SVT_AN_eight"]=""
                        json_make["SVT_AN_nine"]=""
                        json_make["SVT_AN_twenty"]=""
                        json_make["amount_limit_SVT_AN"]=str(total_svt_an_amt)
                        json_make["SVT_LV_eight"]=""
                        json_make["SVT_LV_nine"]=""
                        json_make["SVT_LV_twenty"]=""
                        json_make["amount_limit_SVT_LV"]=str(total_svt_lv_amt)
                        json_list.append(json_make)
                        json_make={}
                        for item in range(len(json_list)):
                            if json_list[item].get("SVT_TE_eight") is None:
                                json_list[item]["SVT_TE_eight"]="-"
                            if json_list[item].get("SVT_TE_nine") is None:
                                json_list[item]["SVT_TE_nine"]="-"
                            if json_list[item].get("SVT_TE_twenty") is None:
                                json_list[item]["SVT_TE_twenty"]="-"
                            if json_list[item].get("amount_limit_SVT_TE") is None:
                                json_list[item]["amount_limit_SVT_TE"]="-"
                            if json_list[item].get("SVT_AN_eight") is None:
                                json_list[item]["SVT_AN_eight"]="-"
                            if json_list[item].get("SVT_AN_nine") is None:
                                json_list[item]["SVT_AN_nine"]="-"
                            if json_list[item].get("SVT_AN_twenty") is None:
                                json_list[item]["SVT_AN_twenty"]="-"
                            if json_list[item].get("amount_limit_SVT_AN") is None:
                                json_list[item]["amount_limit_SVT_AN"]="-"
                            if json_list[item].get("SVT_LV_eight") is None:
                                json_list[item]["SVT_LV_eight"]="-"
                            if json_list[item].get("SVT_LV_nine") is None:
                                json_list[item]["SVT_LV_nine"]="-"
                            if json_list[item].get("SVT_LV_twenty") is None:
                                json_list[item]["SVT_LV_twenty"]="-"
                            if json_list[item].get("amount_limit_SVT_LV") is None:
                                json_list[item]["amount_limit_SVT_LV"]="-"
                        legal_comp["svt"]=json_list          
                    return legal_comp      
        return product_attributes_result
    except Exception as e:
        return product_attributes_result
        #         if len(total_spec)>0:  
        #             spec_id=total_spec[0]
        #             if len(total_namprod)>0:
        #                 namprod=total_namprod[0]
        #             material_list,bdt_list,matstr,material_details=get_material_details_on_selected_spec(spec_id,params)
        #             cas_list,chemical_list,pspec_list,cas_json=get_cas_details_on_selected_spec(spec_id,params)  
        #             if validity is None:
        #                 std_usage=[]
        #                 hundrd_usage=[]
        #                 product_list=[data.replace(" ","\ ") for data in pspec_list if (data!="None" and data!="-")]
        #                 pspec_query=" || ".join(product_list)
        #                 query=f'CSUBI:({pspec_query}) && SUBID:({spec_id})'
        # #                 if sub_category=="Standard, 100 % & INCI Composition":
        # #                     result=list(solr_std_composition.search(query,**params))
        # #                     result_dumps = json.dumps(result)
        # #                     df_std=pd.read_json(result_dumps,dtype=str)
        #                     if "ZUSAGE" in list(df_std.columns):
        #                         std_usage=list(df_std["ZUSAGE"].unique())
        #                     result=[]
        #                     result=list(solr_hundrd_composition.search(query,**params))
        #                     result_dumps = json.dumps(result)
        #                     df_hundrd=pd.read_json(result_dumps,dtype=str)
        #                     if "ZUSAGE" in list(df_hundrd.columns):
        #                         hundrd_usage=list(df_hundrd["ZUSAGE"].unique())           
        #                     usage_catgory=std_usage+hundrd_usage
        #                     json_list=[]
        #                     for i in list(set(usage_catgory)):
        #                         json_make["name"]=i
        #                         json_list.append(json_make)
        #                         json_make={}
        #                     return json_list
        #                 else:
        #                     result=[]
        #                     result=list(solr_legal_composition.search(query,**params))
        #                     result_dumps = json.dumps(result)
        #                     df_legal=pd.read_json(result_dumps,dtype=str)
        #                     if "ZUSAGE" in list(df_legal.columns):
        #                         legal_usage=list(df_legal["ZUSAGE"].unique())
        #                     json_list=[]
        #                     for i in list(set(legal_usage)):
        #                         json_make["name"]=i
        #                         json_list.append(json_make)
        #                         json_make={}
        #                     return json_list              
        #             if validity is not None:
        #                 product_list=[data.replace(" ","\ ") for data in pspec_list if (data!="None" and data!="-")]
        #                 pspec_query=" || ".join(product_list)
        #                 zusage_value=validity.replace(":","\:")
        #                 query=f'CSUBI:({pspec_query}) && ZUSAGE:({zusage_value}) && SUBID:({spec_id})'                       
        #                 if sub_category=="Standard, 100 % & INCI Composition":
        #                     std_result=[]
        #                     hundrd_result=[]
        #                     inci_result=[]
        #                     # if sub_category=="Standard, 100 % & INCI Composition":
        #                     #std composition data
        #                     std_result=list(solr_std_composition.search(query,**params))
        #                     hundrd_result=list(solr_hundrd_composition.search(query,**params))
        #                     total_list=bdt_list+[namprod]
        #                     product_list=[data.replace(" ","\ ") for data in total_list if (data!="None" and data!="-")]
        #                     total_query=" || ".join(product_list)
        #                     params={"rows":2147483647}
        #                     query=f'CATEGORY:CIDP && IS_RELEVANT:1 && PRODUCT:({total_query}) && -PRODUCT_TYPE:null'
        #                     inci_result=list(solr_unstructure_data.search(query,**params))  
        #                     json_make={} 
        #                     for item in cas_json:
        #                         #std table
        #                         std_flag=''
        #                         hundrd_flag=''
        #                         inci_flag='' 
        #                         for std in std_result:
        #                             if std.get("CSUBI").strip()==item.strip():
        #                                 std_flag='s'
        #                                 json_make["std_Componant_Type"]=std.get("COMPT","-")
        #                                 json_make["std_value"]=std.get("CVALU","-")
        #                                 json_make["std_unit"]=std.get("CUNIT","-")
        #                         for hundrd in hundrd_result:
        #                             if hundrd.get("CSUBI").strip()==item.strip():
        #                                 hundrd_flag='s'
        #                                 json_make["hundrd_Componant_Type"]=hundrd.get("COMPT","-")
        #                                 json_make["hundrd_value"]=hundrd.get("CVALU","-")
        #                                 json_make["hundrd_unit"]=hundrd.get("CUNIT","-")
        #                         for inci in inci_result:
        #                             data=json.loads(inci.get("DATA_EXTRACT"))
        #                             inci_cas_number=data.get("CAS Number ").strip()
        #                             if inci_cas_number==cas_json.get(item).get("cas_number").strip():
        #                                 inci_flag='s'
        #                                 json_make["inci_Componant_Type"]="Active"
        #                                 json_make["inci_value_unit"]=data.get("Target Composition","-")
        #                         if std_flag =='':
        #                             json_make["std_Componant_Type"]='-'
        #                             json_make["std_value"]='-'
        #                             json_make["std_unit"]="-"
        #                         if hundrd_flag=='':
        #                             json_make["hundrd_Componant_Type"]="-"
        #                             json_make["hundrd_value"]="-"
        #                             json_make["hundrd_unit"]="-"
        #                         if inci_flag=='':
        #                             json_make["inci_Componant_Type"]="-"
        #                             json_make["inci_value_unit"]="-"
        #                         if std_flag=='s' or hundrd_flag=='s' or inci_flag=='s':
        #                             json_make["pure_spec_Id"]=item
        #                             json_make["cas_Number"]=cas_json.get(item).get("cas_number")
        #                             json_make["ingredient_Name"]=cas_json.get(item).get("chemical_name")
        #                             json_list.append(json_make)
        #                         json_make={}
        #                     # total_std_value=0
        #                     # total_hundrd_value=0
        #                     # total_inci
        #                     # for item in json_list:

        #                     return json_list
        #                 elif sub_category=="Legal Composition":
        #                     json_list=[]
        #                     # query=f'CSUBI:({pspec_query}) && ZUSAGE:({zusage_value}) && SUBID:({spec_id})'
        #                     legal_result=list(solr_legal_composition.search(query,**params))
        #                     legal_svt_spec=[]
        #                     legal_comp={}
        #                     for item in cas_json:           
        #                         for data in legal_result:
        #                             if data.get("CSUBI")==item:
        #                                 json_make["pure_spec_Id"]=item
        #                                 json_make["cas_Number"]=cas_json.get(item).get("cas_number")
        #                                 json_make["ingredient_Name"]=cas_json.get(item).get("chemical_name")
        #                                 legal_svt_spec.append(item)
        #                                 json_make["legal_Componant_Type"]=data.get("COMPT","-")
        #                                 json_make["legal_value"]=data.get("CVALU","-")
        #                                 json_make["legal_unit"]=data.get("CUNIT","-")
        #                                 json_list.append(json_make)
        #                                 json_make={}
        #                     legal_comp["legal_composition"]=json_list
        #                     if validity=='REACH: REG_REACH':
        #                         json_list=[]
        #                         json_make={}
        #                         svt_result=[]
        #                         subid=[data.replace(" ","\ ") for data in legal_svt_spec if (data!="None" and data!="-")]
        #                         subid_list=" || ".join(subid)
        #                         if len(legal_svt_spec)>0:
        #                             query=f'SUBID:({subid_list})'
        #                             svt_result=list(solr_substance_volume_tracking.search(query,**params))
        #                         presence_id=[]
        #                         for data in svt_result:
        #                             presence_id.append(data.get("SUBID","-"))
        #                         for sub in list(set(presence_id)):
        #                             json_make["pure_spec_Id"]=sub
        #                             svt_total_2018=0
        #                             svt_total_2019=0
        #                             svt_total_2020=0
        #                             for data in svt_result:
        #                                 if sub==data.get("SUBID","-"):
        #                                     reg_value=data.get("REGLT","-")
        #                                     reg_year=data.get("QYEAR","-").strip()
        #                                     if reg_value=="SVT_TE":
        #                                         if reg_year=="2018":
        #                                             json_make["SVT_TE_eight"]=data.get("CUMQT","-")
        #                                         if reg_year=="2019":
        #                                             json_make["SVT_TE_nine"]=data.get("CUMQT","-")
        #                                         if  reg_year=="2020":
        #                                             json_make["SVT_TE_twenty"]=data.get("CUMQT","-")
        #                                         json_make["amount_limit_SVT_TE"]=data.get("AMTLT","0")
        #                                     if reg_value=="SVT_AN":
        #                                         if reg_year=="2018":
        #                                             json_make["SVT_AN_eight"]=data.get("CUMQT","-")
        #                                         if reg_year=="2019":
        #                                             json_make["SVT_AN_nine"]=data.get("CUMQT","-")
        #                                         if  reg_year=="2020":
        #                                             json_make["SVT_AN_twenty"]=data.get("CUMQT","-")
        #                                         json_make["amount_limit_SVT_AN"]=data.get("AMTLT","0")
        #                                     if reg_value=="SVT_LV":
        #                                         if reg_year=="2018":
        #                                             svt_total_2018+=float(data.get("CUMQT","-"))
        #                                             json_make["SVT_LV_eight"]=svt_total_2018
        #                                         if reg_year=="2019":
        #                                             svt_total_2019+=float(data.get("CUMQT","-"))
        #                                             json_make["SVT_LV_nine"]=svt_total_2019
        #                                         if  reg_year=="2020":
        #                                             svt_total_2020+=float(data.get("CUMQT","-"))
        #                                             json_make["SVT_LV_twenty"]=svt_total_2020
        #                                         json_make["amount_limit_SVT_LV"]=data.get("AMTLT","0")                   
        #                             json_list.append(json_make)
        #                             json_make={}
        #                         total_svt_te_amt=0
        #                         total_svt_an_amt=0
        #                         total_svt_lv_amt=0
        #                         for item in json_list:
        #                             total_svt_te_amt=total_svt_te_amt+float(item.get("amount_limit_SVT_TE",0))
        #                             total_svt_an_amt=total_svt_an_amt+float(item.get("amount_limit_SVT_AN",0))
        #                             total_svt_lv_amt=total_svt_lv_amt+float(item.get("amount_limit_SVT_LV",0))
        #                         json_make["pure_spec_Id"]="Total"
        #                         json_make["SVT_TE_eight"]=""
        #                         json_make["SVT_TE_nine"]=""
        #                         json_make["SVT_TE_twenty"]=""
        #                         json_make["amount_limit_SVT_TE"]=str(total_svt_te_amt)
        #                         json_make["SVT_AN_eight"]=""
        #                         json_make["SVT_AN_nine"]=""
        #                         json_make["SVT_AN_twenty"]=""
        #                         json_make["amount_limit_SVT_AN"]=str(total_svt_an_amt)
        #                         json_make["SVT_LV_eight"]=""
        #                         json_make["SVT_LV_nine"]=""
        #                         json_make["SVT_LV_twenty"]=""
        #                         json_make["amount_limit_SVT_LV"]=str(total_svt_lv_amt)
        #                         json_list.append(json_make)
        #                         json_make={}
        #                         for item in range(len(json_list)):
        #                             if json_list[item].get("SVT_TE_eight") is None:
        #                                 json_list[item]["SVT_TE_eight"]="-"
        #                             if json_list[item].get("SVT_TE_nine") is None:
        #                                 json_list[item]["SVT_TE_nine"]="-"
        #                             if json_list[item].get("SVT_TE_twenty") is None:
        #                                 json_list[item]["SVT_TE_twenty"]="-"
        #                             if json_list[item].get("amount_limit_SVT_TE") is None:
        #                                 json_list[item]["amount_limit_SVT_TE"]="-"
        #                             if json_list[item].get("SVT_AN_eight") is None:
        #                                 json_list[item]["SVT_AN_eight"]="-"
        #                             if json_list[item].get("SVT_AN_nine") is None:
        #                                 json_list[item]["SVT_AN_nine"]="-"
        #                             if json_list[item].get("SVT_AN_twenty") is None:
        #                                 json_list[item]["SVT_AN_twenty"]="-"
        #                             if json_list[item].get("amount_limit_SVT_AN") is None:
        #                                 json_list[item]["amount_limit_SVT_AN"]="-"
        #                             if json_list[item].get("SVT_LV_eight") is None:
        #                                 json_list[item]["SVT_LV_eight"]="-"
        #                             if json_list[item].get("SVT_LV_nine") is None:
        #                                 json_list[item]["SVT_LV_nine"]="-"
        #                             if json_list[item].get("SVT_LV_twenty") is None:
        #                                 json_list[item]["SVT_LV_twenty"]="-"
        #                             if json_list[item].get("amount_limit_SVT_LV") is None:
        #                                 json_list[item]["amount_limit_SVT_LV"]="-"
        #                         legal_comp["svt"]=json_list
                                
        #                     return legal_comp                   
        
