import os
audit_path = "D:/Users/3539le/Desktop/DATA_VALIDATION_TOOL/Toledo/View_Files/"
#D:\Users\3539le\Desktop\DATA_VALIDATION_TOOL\Whitng\Whitinng_Target_Files
path = os.getcwd()
files = os.listdir(audit_path)
files_xls = [f for f in files if f[-4:] == '.csv']


for i in range(0,len(files_xls)):
        print(files_xls[i]) 
