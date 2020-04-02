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
            df_product_combine=df_product_combine[~df_product_combine["TEXT1"].str.contains(item,case=False,na=False,regex=False)]
        if len(df_product_combine.columns)!=len(product_column):
            dummy=pd.DataFrame([],columns=product_column)
            df_product_combine=pd.concat([df_product_combine,dummy]).fillna("-")
        df_product_combine=df_product_combine.fillna("-")
        return df_product_combine
    except Exception as e:
        return df_product_combine

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
        temp_df=temp_df.replace({"nan":"-"})
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