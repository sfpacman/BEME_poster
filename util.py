import pandas as pd
from pathlib import Path
import requests
import json
from collections import OrderedDict, ChainMap

def get_data(json_dict,filter_key=None,filter_val=None):
    r = requests.post('https://iredcap.csmc.edu/api/',data=json_dict)
    df = pd.DataFrame(r.json())
    if filter_key is not None:
        df=df[df[filter_key] < filter_val] 
    return df

def get_json(report_name,json_base,json_project_id):
    json_out_dict=json.load(open(json_base,"r"))
    json_project_id= json.load(open(json_project_id,"r"))
    #replace report_id with real report id 
    json_out_dict["report_id"]=json_project_id[report_name]
    return json_out_dict    


def get_config_json(project_name,config_dir=None):
    if config_dir is None:
        base_dir = Path(__file__).resolve().parent
        config_dir= base_dir.joinpath('.config')
    json_base=config_dir.joinpath('redcap_report.json')
    json_project_id=config_dir.joinpath('project_id.json')
    config_json=get_json(project_name,json_base,json_project_id)
    return  config_json

def get_data_dict(config_dir=None):
    if config_dir is None:
        base_dir = Path(__file__).resolve().parent
        config_dir= base_dir.joinpath('.config')
    json_base=config_dir.joinpath('redcap_meta.json')
    json_out_dict=json.load(open(json_base,"r"))
    df = get_data(json_out_dict)
    return df

def str_to_dict( s,item_delimiter="|",tuple_delimiter=","):
    out_dict =dict(tuple([ tuple(i.strip().split(tuple_delimiter,maxsplit=1))for i in s.split(item_delimiter) ]))
    return out_dict

def get_recode_dict(select_col_list,data_dict_df, field="field_name" ,recode_field="select_choices_or_calculations"):
    s=data_dict_df[data_dict_df[field].isin(select_col_list)][recode_field]
    s=s[s.str.len()>0].apply(str_to_dict)
    s.index= data_dict_df.iloc[s.index][field]
    return s.to_dict()

def recode_main_df(data_df,data_dict_df):
    select_col=data_df.columns.to_list()
    data_recode_dict=get_recode_dict(select_col,data_dict_df)
    data_df= data_df.replace(data_recode_dict)
    return data_df

def append_key(key,d,append="___"):
    out={ key+append+str(i):j for i,j in d.items() }
    return out 

def get_checkbox_dict():
    out_dict={"1":"yes","0":"no"}
    return out_dict

def recode_check_box(data_df,data_dict_df,field="field_name",col_type="field_type" ,recode_field="select_choices_or_calculations"):
    check_box_regex="___[0-9]$"
    select_col=data_df.filter(regex=check_box_regex).columns.str.replace(check_box_regex,"",regex=True).unique()
    check_type="checkbox"
    
    s=data_dict_df[(data_dict_df[field].isin(select_col)) & (data_dict_df[col_type] == check_type) ][recode_field]
    s=s[s.str.len()>0].apply(str_to_dict)
    s.index= data_dict_df.iloc[s.index][field]
    out_dict=ChainMap(*[ append_key(i,d)  for i,d in s.items() ])
    check_dict = get_checkbox_dict()
    recode_dict= { k :check_dict for k in out_dict.keys( )}
    label_field="field_label" 
    merge_df=pd.DataFrame(out_dict.items(),columns=[field,label_field])
    data_dict_df=data_dict_df.append(merge_df)
    return (recode_dict,data_dict_df)

def append_to_excel(fpath, df, sheet_name):
    with pd.ExcelWriter(fpath, mode="a") as f:
        df.to_excel(f, sheet_name=sheet_name)