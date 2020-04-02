import json
import re
import pandas as pd
import pysolr
import logging
import os
from __app__.shared_code import helper
from __app__.shared_code import settings as config
solr_product=config.solr_product
junk_column=config.junk_column
product_column=config.product_column
product_nam_category=config.product_nam_category
product_rspec_category = config.product_rspec_category
product_namsyn_category = config.product_namsyn_category
material_number_category = config.material_number_category
material_bdt_category = config.material_bdt_category
cas_number_category = config.cas_number_category
cas_pspec_category = config.cas_pspec_category
cas_chemical_category = config.cas_chemical_category
category_with_key=config.category_with_key
category_type = config.category_type
search_category = config.search_category
selected_categories=config.selected_categories
querying_solr_data=helper.querying_solr_data
product_level_creation=helper.product_level_creation
solr_product_params=config.solr_product_params

def all_products(data):
    try:
        logging.info('executing all_product function')
        all_product_list=[]
        search=''
        search_split=''
        search_key=''
        search_value=''
        key_flag=''
        search=data
        key_found=''
        
        if "*" in search:
            key_flag='s'
            search_split=search.split('*')
            search_key=search_split[0]+"*"
            search_value = search_split[1].strip()                                                        
        all_product_list=[]
        if key_flag=='s':
            for key,category,base1,base2,level,combination_category in category_with_key:
                if key==search_key.upper():
                    key_found='s'                                                  
                    if len(search_value)>0: 
                        replace={" ":"\ ","/":"\/","*":"\*"}
                        search_value = search_value.translate(str.maketrans(replace))                      
                        # query=f'TYPE:{base1} && {category}:{search_value}* && -{category}:({ignore_query}) && SUBCT:{base2}'                       
                        query=f'TYPE:{base1} && {category}:*{search_value}* && SUBCT:{base2}'                       
                        df_product_combine=querying_solr_data(query,solr_product_params)
                    else:
                        query=f'TYPE:{base1} && SUBCT:{base2}'
                        df_product_combine=querying_solr_data(query,solr_product_params)
                    all_product_list=all_product_list+product_level_creation(df_product_combine,combination_category,base1,base2,key,level,"yes")                                  
                    break
        if len(search)>=2 and key_found=='': 
            replace={" ":"\ ","/":"\/","*":"\*"}
            search_value = search.translate(str.maketrans(replace))       
            query=f'TEXT1:{search_value}* || TEXT2:{search_value}* || TEXT3:{search_value}*'
            logging.info(query)
            print(query)
            df_product_combine=querying_solr_data(query,solr_product_params)
            rex=re.compile(r"(^{})".format(search_value),re.I)
            for item in search_category:
                edit_df=df_product_combine[df_product_combine[item].str.contains(rex,na=False)]
                if len(edit_df)>0:
                    if item=="TEXT2": 
                        #for real specid 
                        all_product_list=all_product_list+product_level_creation(edit_df,product_rspec_category,"NAMPROD","REAL_SUB","RSPEC*","PRODUCT-LEVEL")
                        #cas level details    
                        all_product_list=all_product_list+product_level_creation(edit_df,cas_pspec_category,"NUMCAS","PURE_SUB","PSEPC*","CAS-LEVEL")
                    elif item=="TEXT1":
                        for ctype in category_type:
                            if ctype=="MATNBR":
                                all_product_list=all_product_list+product_level_creation(edit_df,material_number_category,"MATNBR",'',"MAT*","MATERIAL-LEVEL")
                            elif ctype=="NUMCAS":
                                all_product_list=all_product_list+product_level_creation(edit_df,cas_number_category,"NUMCAS","PURE_SUB","CAS*","CAS-LEVEL")
                            else:
                                all_product_list=all_product_list+product_level_creation(edit_df,product_nam_category,"NAMPROD","REAL_SUB","NAM*","PRODUCT-LEVEL")
                    else:
                        for ctype in category_type:
                            if ctype == "MATNBR":
                                all_product_list=all_product_list+product_level_creation(edit_df,material_bdt_category,"MATNBR",'',"BDT*","MATERIAL-LEVEL")
                            elif ctype == "NAMPROD":
                                all_product_list=all_product_list+product_level_creation(edit_df,product_namsyn_category,"NAMPROD","REAL_SUB","SYN*","PRODUCT-LEVEL")
                            else:
                                all_product_list=all_product_list+product_level_creation(edit_df,cas_chemical_category,"NUMCAS","PURE_SUB","CHEMICAL*","CAS-LEVEL") 
        return all_product_list
    except Exception as e:
        return []
