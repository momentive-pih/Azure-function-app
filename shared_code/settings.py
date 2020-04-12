import pysolr
import os
solr_url_config=os.environ["CUSTOMCONNSTR_SOLRCONNECTIONSTRING"]
#Solar url connection and access
solr_product= pysolr.Solr(solr_url_config+"/product_information/", timeout=10,verify=False)
solr_notification_status=pysolr.Solr(solr_url_config+'/sap_notification_status/', timeout=10,verify=False)
solr_unstructure_data=pysolr.Solr(solr_url_config+'/unstructure_processed_data/', timeout=10,verify=False)
solr_document_variant=pysolr.Solr(solr_url_config+'/sap_document_variant/', timeout=10,verify=False)
solr_ghs_labeling_list_data=pysolr.Solr(solr_url_config+'/sap_ghs_labeling_list_data/', timeout=10,verify=False)
solr_ontology=pysolr.Solr(solr_url_config+'/ontology/',timeout=10,verify=False)
solr_substance_identifier=pysolr.Solr(solr_url_config+'/sap_substance_identifier/',timeout=10,verify=False)
solr_phrase_translation=pysolr.Solr(solr_url_config+'/sap_phrase_translation/',timeout=10,verify=False)
solr_inci_name=pysolr.Solr(solr_url_config+'/inci_name_prod/',timeout=10,verify=False)
solr_std_composition=pysolr.Solr(solr_url_config+'/sap_standard_composition/',timeout=10,verify=False)
solr_hundrd_composition=pysolr.Solr(solr_url_config+'/sap_hundrd_percent_composition/',timeout=10,verify=False)
solr_legal_composition=pysolr.Solr(solr_url_config+'/sap_legal_composition/',timeout=10,verify=False)
solr_substance_volume_tracking=pysolr.Solr(solr_url_config+'/sap_substance_volume_tracking/',timeout=10,verify=False)

# Internationalization
junk_column=["solr_id","_version_"]
product_column = ["TYPE","TEXT1","TEXT2","TEXT3","TEXT4","SUBCT"]
product_nam_category = [["TEXT1","NAM PROD"],["TEXT2","REAL-SPECID"],["TEXT3","SYNONYMS"]]
product_rspec_category = [["TEXT2","REAL-SPECID"],["TEXT1","NAM PROD"],["TEXT3","SYNONYMS"]]
product_namsyn_category = [["TEXT3","SYNONYMS"],["TEXT2","REAL-SPECID"],["TEXT1","NAM PROD"]]
material_number_category = [["TEXT1","MATERIAL NUMBER"],["TEXT3","BDT"],["TEXT4","DESCRIPTION"]]
material_bdt_category = [["TEXT3","BDT"],["TEXT1","MATERIAL NUMBER"],["TEXT4","DESCRIPTION"]]
cas_number_category = [["TEXT1","CAS NUMBER"],["TEXT2","PURE-SPECID"],["TEXT3","CHEMICAL-NAME"]]
cas_pspec_category = [["TEXT2","PURE-SPECID"],["TEXT1","CAS NUMBER"],["TEXT3","CHEMICAL-NAME"]]
cas_chemical_category = [["TEXT3","CHEMICAL-NAME"],["TEXT2","PURE-SPECID"],["TEXT1","CAS NUMBER"]]
category_with_key=[["NAM*","TEXT1","NAMPROD","REAL_SUB","PRODUCT-LEVEL",product_nam_category],
                ["RSPEC*","TEXT2","NAMPROD","REAL_SUB","PRODUCT-LEVEL",product_rspec_category],
                ["SYN*","TEXT3","NAMPROD","REAL_SUB","PRODUCT-LEVEL",product_namsyn_category],
                ["MAT*","TEXT1","MATNBR","REAL_SUB","MATERIAL-LEVEL",material_number_category],
                ["BDT*","TEXT3","MATNBR","REAL_SUB","MATERIAL-LEVEL",material_bdt_category],
                ["CAS*","TEXT1","NUMCAS","PURE_SUB","CAS-LEVEL",cas_number_category],
                ["CHEM*","TEXT3","NUMCAS","PURE_SUB","CAS-LEVEL",cas_chemical_category], 
                ["PSPEC*","TEXT2","NUMCAS","PURE_SUB","CAS-LEVEL",cas_pspec_category],            
                ["SPEC*","TEXT2","NAMPROD","REAL_SUB","PRODUCT-LEVEL",product_rspec_category],
                ["SPEC*","TEXT2","NUMCAS","PURE_SUB","CAS-LEVEL",cas_pspec_category]]
# dict_category={
#     "NAM*":{

#     }
# }
category_type = ["MATNBR","NUMCAS","NAMPROD"]
search_category = ["TEXT1","TEXT2","TEXT3"]
selected_categories=["BDT*","MAT*","NAM*","CAS*","CHEM*","RSPEC*","PSPEC*","SYN*","SPEC*"]
solr_product_column = ",".join(product_column)
max_rows=2147483647
solr_product_params={"rows":max_rows,"fl":solr_product_column}
pipe_delimitter=" | "
or_delimiter=" || "
hypen_delimiter=" - "
comma_delimiter=", "
ag_registration_country={"EU_REG_STATUS":"EU Region","US_REG_STATUS":"US Canada","LATAM_REG_STATUS":"Latin America"}
ag_registration_list=["EU_REG_STATUS","US_REG_STATUS","LATAM_REG_STATUS"]
us_eu_category={"US-FDA":"US FDA Letter","EU-FDA":"EU Food Contact"}       
restricted_dict={"GADSL":"GADSL","CALPROP":"CAL-PROP"}
restricted_sub_list=["GADSL","CAL-PROP"]
report_column_str="SUBID,REPTY,RGVID,LANGU,VERSN,STATS,RELON"
unstructure_column_str="PRODUCT_TYPE,SPEC_ID,CREATED,DATA_EXTRACT,CATEGORY,PRODUCT,UPDATED"
notification_column_str="NOTIF,ZUSAGE,ADDIN,RLIST,SUBID"
phrase_column_str="PHRKY,PTEXT"
BlobPath=f"https://devstorpih001.blob.core.windows.net/"
ghs_image_path=BlobPath+f"momentive-sources-pih/ghs-images-pih/"
sas_token=f"?sv=2019-02-02&ss=b&srt=sco&sp=rl&se=2020-05-29T20:19:29Z&st=2020-04-02T12:19:29Z&spr=https&sig=aodIg0rDPVsNEJY7d8AerhD79%2FfBO9LZGJdx2j9tsCM%3D"
home_icon_product_attributes=BlobPath+f"momentive-sources-pih/home-page-icon-images-pih/productAttributes.jpg"+sas_token
home_icon_customer_communication=BlobPath+f"momentive-sources-pih/home-page-icon-images-pih/customerCommunication.png"+sas_token
home_icon_product_compliance=BlobPath+f"momentive-sources-pih/home-page-icon-images-pih/ProductCompliance.jpg"+sas_token
home_icon_report_data=BlobPath+f"momentive-sources-pih/home-page-icon-images-pih/report.png"+sas_token
home_icon_restricted_substance=BlobPath+f"momentive-sources-pih/home-page-icon-images-pih/restrictedsubstance.png"+sas_token
home_icon_sales_info=BlobPath+f"momentive-sources-pih/home-page-icon-images-pih/salesInformation.jpg"+sas_token
home_icon_toxicology=BlobPath+f"momentive-sources-pih/home-page-icon-images-pih/toxicology.jpg"+sas_token
