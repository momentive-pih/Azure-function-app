import pandas as pd
import json
import logging
from . import settings as config
solr_product=config.solr_product
product_column = config.product_column
unwanted_fields=["obsolete"]

def querying_solr_data(query,params):
    try:
        df_product_combine=pd.DataFrame()      
        response = solr_product.search(query,**params)
        result = json.dumps(list(response))
        df_product_combine=pd.read_json(result,dtype=str)
        for item in unwanted_fields:
            # unwanted_df=df_product_combine[df_product_combine["TEXT1"].str.contains(item,case=False,na=False,regex=False)]
            # real_spec=list(unwanted_df["TEXT2"].unique())
            df_product_combine=df_product_combine[~df_product_combine["TEXT1"].str.contains(item,case=False,na=False,regex=False)]
        if len(df_product_combine.columns)!=len(product_column):
            dummy=pd.DataFrame([],columns=product_column)
            df_product_combine=pd.concat([df_product_combine,dummy])
        df_product_combine=df_product_combine.fillna("-")   
        df_product_combine=df_product_combine.replace({"nan":"-"})
        return df_product_combine
    except Exception as e:
        return df_product_combine

def get_data_from_core(core,query,params):
    params["rows"]=config.max_rows
    core_df=pd.DataFrame()  
    response = core.search(query,**params)
    data_list=list(response)
    result = json.dumps(data_list)
    core_df=pd.read_json(result,dtype=str)
    core_df=core_df.replace({"nan":"-"})
    return data_list,core_df


def product_level_creation(product_df,product_category_map,type,subct,key,level_name,filter_flag="no"):
    try:
        json_list=[]
        if filter_flag=="no":
            if type !='' and subct !='':
                temp_df=product_df[(product_df["TYPE"]==type) & (product_df["SUBCT"]==subct)]
            else:
                temp_df=product_df[(product_df["TYPE"]==type)]
        else:
            temp_df=product_df
        
        temp_df.drop_duplicates(inplace=True)
        # temp_df=temp_df.replace({"nan":"-"})
        total_count=0
        display_category=''
        json_category=''
        extract_column=[]
        for column,category in product_category_map:
            try:
                extract_column.append(column)               
                col_count=list(temp_df[column].unique())
                if '-' in col_count:
                    col_count = list(filter(('-').__ne__, col_count))
                category_count = len(col_count)
                total_count+=category_count
                display_category+=category+" - "+str(category_count)+" | "
                json_category+= category+" | " 
            except Exception as e:
                pass
        display_category=display_category[:-3] 
        json_category=json_category[:-3]       
        temp_df=temp_df[extract_column].values.tolist()
        for value1,value2,value3 in temp_df:
            value = str(value1).strip() + " | "+str(value2).strip()+" | "+str(value3).strip()
            out_dict={"name":value,"type":json_category,"key":key,"group":level_name+" ("+display_category+")"+" - "+str(total_count) }
            json_list.append(out_dict)
        # print(json_list)
        return json_list
    except Exception as e:
        return json_list

def replace_character_for_querying(value_list):
    replace={" ":"\ ","/":"\/","*":"\*"}
    replaced_list=[data.translate(str.maketrans(replace)) for data in value_list if data!=None]
    replaced_query=" || ".join(replaced_list)
    return replaced_query

def finding_cas_details_using_real_specid(product_rspec,params):
    product_rspec=" || ".join(product_rspec)
    query=f'TYPE:SUBIDREL && TEXT2:({product_rspec}) && SUBCT:REAL_SUB'
    spec_rel_df=querying_solr_data(query,params) 
    spec_rel_list=spec_rel_df[["TEXT1","TEXT2"]].values.tolist()
    column_value = list(spec_rel_df["TEXT1"].unique())
    spec_query=" || ".join(column_value)
    query=f'TYPE:NUMCAS && SUBCT:PURE_SUB && TEXT2:({spec_query})'
    cas_df=querying_solr_data(query,params)                 
    #real spec will act as pure spec componant
    query=f'TYPE:NUMCAS && TEXT2:({product_rspec})'
    real_pure_spec_df=querying_solr_data(query,params)
    cas_df=pd.concat([cas_df,real_pure_spec_df])
    return cas_df,spec_rel_list

def finding_product_details_using_real_specid(product_rspec,params):
    product_rspec=" || ".join(product_rspec)
    query=f'TYPE:NAMPROD && SUBCT:REAL_SUB && TEXT2:({product_rspec})'
    prod_df=querying_solr_data(query,params)
    return prod_df

def finding_material_details_using_real_specid(product_rspec,params):
    product_rspec=" || ".join(product_rspec)
    query=f'TYPE:MATNBR && TEXT2:({product_rspec})'
    material_df=querying_solr_data(query,params)
    return material_df

def spec_constructor(req_body):
    try:
        last_specid=''
        namlist=[]
        speclist_data=[]
        speclist_json={}
        total_namprod=[]
        total_spec=[]
        spec_body=req_body.get("Spec_id")
        for item in spec_body:           
            spec_details=item.get("name").split(" | ")
            spec_id=spec_details[0]
            namprod=spec_details[1]
            if spec_id!='':
                total_spec.append(spec_id)
            if (last_specid!=spec_id) and last_specid!='':
                namstr=", ".join(namlist)
                speclist_json[last_specid]=namstr
                namlist=[]
                namlist.append(namprod)
                total_namprod.append(namprod)            
            else:
                namlist.append(namprod)  
                total_namprod.append(namprod)           
            last_specid=spec_id
        namstr=", ".join(namlist)
        speclist_json[last_specid]=namstr
        return speclist_data,speclist_json,list(set(total_spec)),list(set(total_namprod))
    except Exception as e:
        return speclist_data,speclist_json,list(set(total_spec)),list(set(total_namprod))

def construct_common_level_json(common_json):
    MaterialLevel = [{
    "material_Number": "000000000000017718",
    "description": "^LSR 2650 A",
    "bdt": "LSR 2650 A",
    "real_Spec_Id":[ "000000018924 | LSR 2650 A","000000018905 | LSR 2640 B","000000018905 | LSR 2640 A"]
    },
    { 
    "material_Number": "000000000000017719",
    "description": "^LSR 2650 A-200K",
    "bdt": "LSR 2650 A",
    "real_Spec_Id": ["000000018924 | LSR 2650 A","000000018905 | LSR 2640 B","000000018905 | LSR 2640 A"]
    },{ 
    "material_Number": "000000000000017720",
    "description": "^LSR 2650 A-200K",
    "bdt": "LSR 3650 A",
    "real_Spec_Id": ["000000018924 | LSR 2650 A"]
    }
    ]

    productLevel = [
    {"real_Spec_Id": "000000018924", "namprod": "LSR 2650 A", "synonyms": "-"},
    {"real_Spec_Id": "000000018905", "namprod": "LSR 2640 B", "synonyms": "-"}
    ]

    CasLevel = [ 
    {
    "pure_Spec_Id": "000000002910",
    "cas_Number": "7732-18-5",
    "chemical_Name": "Water",
    "real_Spec_Id":["000000018905 | LSR 2640 A","000000018905 | LSR 2640 B", "000000018924 | LSR 2650 A", "000000018925 | LSR 2650 B", "000000019160 | EMU101", "000000019451 | EMU120"]
    },{
    "pure_Spec_Id": "000000002910",
    "cas_Number": "7732-18-6",
    "chemical_Name": "Hydrochloric",
    "real_Spec_Id":["000000018905 | LSR 2640 A","000000018905 | LSR 2640 B", "000000018924 | LSR 2650 A", "000000018925 | LSR 2650 B", "000000019160 | EMU101", "000000019451 | EMU120"]
    }
    ]

    selectedSpecList =[ 
    {"id": 1, "name": "000000018905 | LSR 2640 A"},
    {"id": 2, "name": "000000018905 | LSR 2640 B"},
    {"id": 3, "name": "000000018924 | LSR 2650 A"}
    ]

    # json_array={
    #     'Spec_id': selectedSpecList,
    #     'Category_details': '',
    #     'product_Level':productLevel,
    #     'Mat_Level':MaterialLevel
    #     }
    json_array=common_json
    all_details={}
    spec_list=[]
    material_list=[]
    last_specid=''
    for item in json_array.get("Spec_id"):
        spec_nam_id=item.get("name")
        spec_id_split=item.get("name").split(config.pipe_delimitter)
        if len(spec_id_split)>0:
            spec_id=spec_id_split[0].strip()
            if last_specid!=spec_id:
                spec_list.append(spec_id)
                all_details[spec_id]={}
            else:
                continue
        #product level classify
        if(json_array.get("product_Level")):
            for prod in json_array.get("product_Level"):
                prod_spec=prod.get("real_Spec_Id")
                synonyms=prod.get("synonyms").strip()
                namprod=prod.get("namprod").strip()
                if prod_spec==spec_id:
                    all_details=item_arrange(all_details,prod_spec,"namprod",namprod)  
                    all_details=item_arrange(all_details,prod_spec,"synonyms",synonyms)
                    print(all_details)
        #material level classify
        if(json_array.get("Mat_Level")): 
            for matid in json_array.get("Mat_Level"):
                mat_spec_list=matid.get("real_Spec_Id")
                bdt=matid.get("bdt")
                material_number=matid.get("material_Number")
                material_list.append(material_number)
                if spec_nam_id in mat_spec_list:
                    all_details=item_arrange(all_details,spec_id,"material_number",material_number)
                    all_details=item_arrange(all_details,spec_id,"bdt",bdt)
        #cas level classify
        if(json_array.get("CAS_Level")): 
            for casid in json_array.get("CAS_Level"):
                cas_spec_list=casid.get("real_Spec_Id")
                pure_spec=casid.get("pure_Spec_Id")
                cas_number=casid.get("cas_Number")
                chemical_name=casid.get("chemical_Name")
                if spec_nam_id in cas_spec_list:
                    all_details=item_arrange(all_details,spec_id,"pure_spec_id",pure_spec)
                    all_details=item_arrange(all_details,spec_id,"cas_number",cas_number)
                    all_details=item_arrange(all_details,spec_id,"chemical_name",chemical_name)
        last_specid=spec_id
    print(all_details)
    return all_details,spec_list,list(set(material_list))
    
def item_arrange(all_details,spec_id,prod_type,prod_value):
    if(all_details.get(spec_id).get(prod_type)):
        prod_list=all_details.get(spec_id).get(prod_type)
        if prod_value != "-" and len(prod_list)>0:
            if prod_value not in prod_list:
                prod_list.append(prod_value)
                # prod_list=list(set(prod_list))
                all_details[spec_id][prod_type]=prod_list
    else:
        all_details[spec_id][prod_type]=[]
        if prod_value != "-":
            all_details[spec_id][prod_type].append(prod_value)
    return all_details

def unstructure_template(all_details,category):
    try:
        unstructure_query=''
        # category=["US-FDA","EU-FDA"]
        product_map={"namprod":"NAMPROD","bdt":"BDT","material_number":"MATNBR","cas_number":"NUMCAS"}
        product_section_list=[]
        or_demiliter=config.or_demiliter
        spec_id_section=''
        spec_id_section_list=[]
        product_query=''
        category_query=or_demiliter.join(category)
        for specid in all_details:
            spec_query=f'SPEC_ID:*{specid}*'
            for prod_type in all_details.get(specid):
                if prod_type in product_map:
                    product_type_query=f'PRODUCT_TYPE:{product_map.get(prod_type)}'
                    product_list=all_details.get(specid).get(prod_type)
                    replaced_query=replace_character_for_querying(product_list)
                    product_value_query=f'PRODUCT:({replaced_query})'
                    product_query=f'({product_type_query} && {product_value_query})'
                    product_section_list.append(product_query)
            if len(product_section_list)!=0:
                product_section_template=or_demiliter.join(product_section_list)
                spec_id_section=f'({spec_query} && ({product_section_template}))'
                spec_id_section_list.append(spec_id_section)
        if len(spec_id_section_list)>0:
            spec_id_section_query=or_demiliter.join(spec_id_section_list)
            unstructure_query=f'IS_RELEVANT:1 && CATEGORY:({category_query}) && ({spec_id_section_query})'
    except Exception as e:
        print(e)
    return unstructure_query

def finding_spec_details(spec_list,unstructure_spec):
    result_spec=[]
    if ";" in unstructure_spec:
        for id in spec_list:
            if id in unstructure_spec:
                result_spec.append(id)
    else:
        result_spec.append(unstructure_spec) 
    return (config.pipe_delimitter).join(result_spec)


    
