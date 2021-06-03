import sys 
import os
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile
import numpy as np
import hashlib 

def data_validation_tool_with_key(filepath1,filepath2,key1,key2,outputpath):
    f1 = filepath1
    f2 = filepath2
    key_in1 = key1
    key_in2 = key2
    out_path = outputpath
    table_name = f2
    #reading input files
    f1 = pd.read_csv(f1,delimiter=',',low_memory=False);
    f2 = pd.read_csv(f2,delimiter='|',low_memory=False);

    #dropping audit columns
    f1 = f1.drop(columns = f1.columns[-1])
    f2 = f2.drop(columns = f2.columns[[-1,-2]])
    
    #replacing NULL values with "null"
    f1_null = f1.replace(to_replace = np.nan, value = "null"); 
    f2_null = f2.replace(to_replace = np.nan, value = "null"); 
    
    #f1_null[key_in1] = f1_null[key_in1].astype(float)
    #f1_null[key_in1] = f1_null[key_in1].astype(str)
    #f2_null[key_in2] =  f2_null[key_in2].astype(str)
    #pulling out only macthed primary key records
    r1 = pd.merge(f1_null, f2_null, how ='inner', left_on=[key_in1],right_on=[key_in2])
    
    
    #getting column count of f1 and r1
    columns_count_join_result1 = f1.shape[1];
    columns_count_join_result2 = r1.shape[1];
    
    #dividing r1 into two data frames 
    join_result_matched_1 = r1[r1.columns[0:columns_count_join_result1]]
    join_result_matched_2 = r1[r1.columns[columns_count_join_result1:columns_count_join_result2]]
    
    #sorting two data frames based on primary key
    s1=join_result_matched_1.sort_values(key_in1, axis = 0, ascending = True)
    s2=join_result_matched_2.sort_values(key_in2, axis = 0, ascending = True)
    
    no_prim_key_matched = r1.shape[0]
    
    #Generating hash values for two dataframes     
    h1=pd.util.hash_pandas_object(s1)
    h2=pd.util.hash_pandas_object(s2)
    
    #assinging column names for hashed data frames
    h1=pd.DataFrame(h1,columns=['tcol1'])
    h2=pd.DataFrame(h2,columns=['scol1'])
    
    #appending hash column to main data frames 
    s1['tcol1']=h1['tcol1']
    s2['scol1']=h2['scol1']
    
    #comparing and pulling odd rows based on hash primary key 
    sr1 = pd.merge(s1, s2, how ='outer', left_on=['tcol1'],right_on=['scol1'])
    sr2=sr1[sr1['scol1'].isnull()]
    sr3=sr1[sr1['tcol1'].isnull()]
    
    #removing nulled columns on sr2 and sr3 data frames
    l1 = sr2[sr2.columns[0:columns_count_join_result1]]
    l2 = sr3[sr3.columns[columns_count_join_result1+1:columns_count_join_result2+1]]
    
    #converting data frames into two dimensional array
    join_result_matched_1_arr = l1.to_numpy();
    join_result_matched_2_arr = l2.to_numpy();
    
    #getting column and row count of data frames
    join_result_matched_1_row_count = l2.shape[0];
    join_result_matched_1_col_count = l1.shape[1];
    
    #fetching primary key position
    enrty_loc = l1.columns.get_loc(key_in1)
    
    #for loop to fing the odd column values for each primary key
    list = []
    
    for i in range(0,join_result_matched_1_row_count):
        dum =''
        for k in range(0,join_result_matched_1_row_count):
            caught = 0
            if(join_result_matched_1_arr[i][enrty_loc] == join_result_matched_2_arr[k][enrty_loc]):
                caught = k
                dum = dum + str(join_result_matched_2_arr[i][enrty_loc])+','
                for j in range(0,join_result_matched_1_col_count):
                    if(join_result_matched_1_arr[i][j] != join_result_matched_2_arr[caught][j]):
                        dum = dum + str(join_result_matched_2.columns[j]) + ","
        list.append(dum)
    
    unmatched1 = pd.merge(f1_null, f2_null, how ='outer', left_on=[key_in1],right_on=[key_in2])
    join_result1 = unmatched1[unmatched1[key_in1].isnull()].dropna(axis =1) 
    join_result2 = unmatched1[unmatched1[key_in2].isnull()].dropna(axis =1)
    join_result1 = join_result1[key_in2].T
    join_result2 = join_result2[key_in1].T
    join_result3 = join_result1.append(join_result2)
    join_result3 = pd.DataFrame(join_result3)
    
    
    SOURCE_COUNT = f1.shape[0]
    VIEW_COUNT = f2.shape[0]
    matched_count = no_prim_key_matched-join_result_matched_1_row_count
    if(matched_count == 0):
        unmatched_count =  SOURCE_COUNT + VIEW_COUNT
    else:
        unmatched_count = join_result_matched_1_row_count + join_result3.shape[0]
    final_output = pd.DataFrame(list)
    final_output = final_output.append(join_result3)
    output_file_name = str(table_name).split("/")
    output_file_name = output_file_name[len(output_file_name)-1].replace('.csv','')
    survey = [output_file_name,SOURCE_COUNT,VIEW_COUNT,matched_count,unmatched_count]
    audit_df = pd.DataFrame(survey,index = ["Table_Name","SOURCE_COUNT","VIEW_COUNT","Matched_Count","Unmatched_Count"],columns = ['Count'])
    output_path = out_path + output_file_name+"_key.csv"
    audit_path = out_path + output_file_name+"_audit"+".xlsx"
    final_output.to_csv(output_path,index = False,header=False,sep='\t')
    audit_df = audit_df.T
    audit_df.to_excel(audit_path,index = True,header=True)
    

def data_validation_tool_without_key(filepath1,filepath2,outputpath):
    f1 = filepath1
    f2 = filepath2
    out_path = outputpath
    table_name = f2
    #reading input files
    f1 = pd.read_csv(f1,delimiter=',');
    f2 = pd.read_csv(f2,delimiter='|');
    
    #dropping audit columns
    f1 = f1.drop(columns = f1.columns[-1])
    f2 = f2.drop(columns = f2.columns[[-1,-2]])
    
    #replacing NULL values with "null"
    f1_null = f1.replace(to_replace = np.nan, value = "null"); 
    f2_null = f2.replace(to_replace = np.nan, value = "null"); 
    
    #f1_null['table'] = 'table1'
    #f2_null['table'] = 'table2'
    
    input1_arr = f1_null.to_numpy();
    input2_arr = f2_null.to_numpy();
    
    
    rows_count_1 =  f1_null.shape[0];
    rows_count_2 =  f2_null.shape[0];
    columns_count_1 = f1_null.shape[1];
    columns_count_2 = f2_null.shape[1];
    
    md5_1 = 'md5_1'     
    md5_2 = 'md5_2'
    table1_result = [] 
    table2_result = []
    
    for i  in range(0,rows_count_1):
        temp = ''
        for j in range(0,columns_count_1):
            temp = temp + str(input1_arr[i][j])
        
        temp3 = hashlib.md5(temp.encode())
        table1_result.append(temp3.hexdigest())
        table_1 = pd.DataFrame(table1_result, columns = [md5_1])
        
    for i  in range(0,rows_count_2):
        temp = ''
        for j in range(0,columns_count_2):
            temp = temp + str(input2_arr[i][j])
        
        temp3 = hashlib.md5(temp.encode())
        table2_result.append(temp3.hexdigest())
        table_2 = pd.DataFrame(table2_result,columns = [md5_2])  
    
    
    md5_append_data_table1 =  f1_null
    md5_append_data_table1[md5_1] = table_1
    
    md5_append_data_table2 =  f2_null
    md5_append_data_table2[md5_2] = table_2
     
    
    
    r1 = pd.merge(f1_null, f2_null, how ='outer', left_on=[md5_1],right_on=[md5_2])
    r2 = pd.merge(f1_null, f2_null, how ='inner', left_on=[md5_1],right_on=[md5_2])
    
    Unmatched1 = r1[r1[md5_2].isnull()].dropna(axis =1) 
    Unmatched1['Table'] = "Source"
    Unmatched2 = r1[r1[md5_1].isnull()].dropna(axis =1)
    Unmatched2['Table'] = "View"
    
    if(Unmatched1.shape[0] == 0):
        consolidated = pd.DataFrame(Unmatched2,columns=Unmatched2.columns)
    elif(Unmatched2.shape[0] == 0 ):
        consolidated = pd.DataFrame(Unmatched1,columns=Unmatched1.columns)
    else:
        consolidated = pd.DataFrame(np.concatenate([Unmatched1.values, Unmatched2.values]),columns=Unmatched1.columns)
    
       
    SOURCE_COUNT = f1.shape[0]
    VIEW_COUNT = f2.shape[0]
    unmatched_count = consolidated.shape[0]
    matched_count = r2.shape[0]
    final_output = pd.DataFrame(consolidated)
    output_file_name = str(table_name).split("/")
    output_file_name = output_file_name[len(output_file_name)-1].replace('.csv','')
    survey = [output_file_name,SOURCE_COUNT,VIEW_COUNT,matched_count,unmatched_count]
    audit_df = pd.DataFrame(survey,index = ["Table_Name","SOURCE_COUNT","VIEW_COUNT","Matched_Count","Unmatched_Count"],columns = ['Count'])
    output_path = out_path + output_file_name+"_no_key.xlsx"
    audit_path = out_path + output_file_name+"_audit"+".xlsx"
    final_output.to_excel(output_path,index = False)
    audit_df = audit_df.T
    audit_df.to_excel(audit_path,index = True,header=True)    
    
        
def parse_params(params_path):
    params = pd.read_excel(params_path)
    params = params.replace(to_replace = np.nan, value = "null");
    params_arr = params.to_numpy();
    params_row_count = params.shape[0];
    for i in range(0,params_row_count):
        if(params_arr[i][2] != "null" and params_arr[i][3] != "null"):
            data_validation_tool_with_key(params_arr[i][0],params_arr[i][1],params_arr[i][2],params_arr[i][3],params_arr[i][4])
        elif(params_arr[i][2] == "null" and params_arr[i][3] == "null"):
            data_validation_tool_without_key(params_arr[i][0],params_arr[i][1],params_arr[i][4])
        
def audit_consolidation(params_path):
    params = pd.read_excel(params_path)
    params_arr = params.to_numpy();
    audit_path = params_arr[0][4]
    path = os.getcwd()
    files = os.listdir(audit_path)
    files_xls = [f for f in files if f[-11:] == '_audit.xlsx']
    df = pd.DataFrame()
    for i in range(0,len(files_xls)):
        filename = audit_path+files_xls[i] 
        data = pd.read_excel(filename, 'Sheet1')
        df = df.append(data)
        os.remove(filename)
        
    final_audit_path = audit_path+"final_audit.xlsx"
    df.to_excel(final_audit_path,index = False)
    

parse_params(sys.argv[1])
audit_consolidation(sys.argv[1])