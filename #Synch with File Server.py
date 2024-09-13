#Synch with file server
#This script synchronizes two directorys or whatever you want
#Author: KeineSchere
#License: GPLv3 // https://www.gnu.org/licenses/gpl-3.0.de.html
from IPython.display import display
import os
import pandas as pd
import time
import shutil
from datetime import datetime

#Replace with your directory path you want to copy
Synch_From = r'D:/ExampleCopyFrom/'
#Replace with your directory path you want to copy to
Synch_To   = r'Y:/ExampleCopyTo/'
#Replace with the location you want to safe the log
Safe_Log   = r'Y:ExampleSafeLocation/'

ServerDF = pd.DataFrame(columns=['Name', 'Created2', 'Modified2', 'Path2', 'Parent2'])
NameSe   = 'ServerDF'
WinDF    = pd.DataFrame(columns=['Name', 'Created', 'Modified', 'Path', 'Parent', 'Modded'])
NameWIN  = 'WinDF'
LogDF    = pd.DataFrame(columns=['Action, Time, Info'])
CountServer = 0
CountWin = 0 
TotalCount = 0
global zehner
zehner = 10

def Get_Data(dir_path, DF, count, Name):
    for path in os.listdir(dir_path):
        global TotalCount
        TotalCount += 1
        count += 1

        File_Path = dir_path + path 
                
        if 'desktop.ini' in File_Path or 'System Volume' in File_Path or '$RECYCLE' in File_Path or 'DONT_SYNCH_DIR' in File_Path:
            LogDF._append({'Action':'Cancel Access', 
                            'Time': time, 
                            'Info': 'Windows Trash'}, ignore_index=True)
            print('Fuck Windows!')
                    
        else: 
        
            ti_c = os.path.getctime(File_Path)
            ti_m = os.path.getmtime(File_Path)

            # Converting the time in seconds to a timestamp
            c_ti = time.ctime(ti_c)
            m_ti = datetime.fromtimestamp(ti_m).strftime('%Y-%m-%d %H:%M:%S')
        
            if os.path.isfile(File_Path) == True:
                if Name == 'WinDF':
                    DF.loc[len(DF.index)] = [path, c_ti, m_ti, File_Path, dir_path, '']  
                        
                else:
                    DF.loc[len(DF.index)] = [path, c_ti, m_ti, File_Path, dir_path]
                        
            elif os.path.isdir(File_Path) == True:
                if Name == 'WinDF':
                    DF.loc[len(DF.index)] = [path, c_ti, m_ti, File_Path, dir_path, '']  
                        
                else:
                    DF.loc[len(DF.index)] = [path, c_ti, m_ti, File_Path, dir_path]
                
                File_Path = File_Path + '/'
                Get_Data(File_Path, DF, count, Name)
                print(f"{count} Objects were Counted in Directory: ", dir_path)
    
def Set_Flag(MainDF):
    print("\nStart of Set_Flag")
    for ind in range(len(MainDF)):
        #File changed
        try:
            #Check if is a file to update
            if os.path.isfile(MainDF.loc[ind, "Path"]):
                if (MainDF.loc[ind, "Modified"]) >= (MainDF.loc[ind, 'Modified2']):
                    MainDF.loc[ind, "Modded"] = 'M'
                    print ("Found modded: ",MainDF.Name[ind])
        except:
            print("Catched")        
    print("End of Set_Flag\n")

def Check_Change(MainDF):
    print("\nStart of Check_Changed")
    for Modified in MainDF.index:
        try: 
            if (MainDF['Modded'][Modified] == 'M'):
                #Delete old file and uploade new file
                print("Try Modifying")

                Source_Path = (MainDF['Path'][Modified])
                print(Source_Path)
                Dest_Path   = (MainDF['Parent2'][Modified])
                
                LogDF._append({'Action':'Updated File', 
                               'Time': time, 
                               'Info': (MainDF['Path'][Modified])}, ignore_index=True)
                   
                #Delte modified file and copy new version of the file
                Del_File = os.path.join(MainDF['Parent2'][Modified],MainDF['Name'][Modified])
                os.remove(Del_File)
                print ("Removed old Version")
                shutil.copy(str(Source_Path), str(Dest_Path))
                print ("Succesfully uploaded new Version")
        except:
            print("Catched")
            LogDF._append({'Action':'Update File ERROR', 
                            'Time': time, 
                            'Info': (WinDF['Name'][Modified])}, ignore_index=True)
    print("End of Check_Changed\n")
    
def Create_Log(MainDF, LogDF):
    print("\nStart of creating Log")
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d_%H-%M-%S")
    Safe_Path = Safe_Log + "Log" + str(current_time)
    #Create a Log-File for the whole Dic. structure and changed files
    LogDF.to_csv(Safe_Path, index=False)
    MainDF.to_csv(Safe_Path, index=False, mode="a")
    #Creates a txt file / Not recommended just use a f*cking csv!
    #with open(str(Safe_Path), "x") as Log:
    #    LogDF = LogDF.to_string(header=False, index=False)
    #    WinDF = WinDF.to_string(header=True, index=True)
    #    Write_Log = "Win:" ,WinDF, "Server:", ServerDF
    #    Log.write(str(Write_Log))
    print("End of creating Log\n")
    
def Check_Exist(WinDF, ServerDF):
    print("\nStart of check_exist")
    global zehner
    zehner = 0
    
    for index, row in WinDF.iterrows():
        show_progress(index, TotalCount_1)
        
        try:
            check = ""
            Pa  = row.Path.replace(Synch_From, "")
               
            for index, row2 in ServerDF.iterrows():
                try:
                    Pa2 = row2.Path2.replace(Synch_To, "")
                        
                    if (row2.Name == row.Name and Pa2 == Pa):
                        #Found the same dir on server / no synch requierd
                        check = "X"
                        break
                except:
                    print("Error")
                
            #Create new file/dir
            if (check == ""):
                New_Path = Synch_To + Pa 
                print(New_Path)
                    
                if os.path.isdir(row.Path):
                    print("Creating directory: ", New_Path)
                    os.makedirs(New_Path)
                        
                    LogDF._append({'Action':'Created new dir', 
                                     'Time': time, 
                                     'Info': (WinDF['Name']["new dir"])}, ignore_index=True)
                        
                elif os.path.isfile(row.Path):
                    print("Copying file: ", New_Path)
                    shutil.copy(str(row.Path), str(New_Path))
                    LogDF._append({'Action':'Copied new file', 
                                     'Time': time, 
                                     'Info': (WinDF['Name']["new file"])}, ignore_index=True)
        except:
            print("Error_Check_Exist")
    print("End of check_exist\n")
        
def Junk_detection(WinDF, ServerDF):
    print("\nStart of Junk_Detection")
    for index, row2 in ServerDF.iterrows():    
        show_progress(index, TotalCount_2)
        
        try: 
            Pa2 = row2.Path2.replace(Synch_To, "")
            check = ""
            for index, row in WinDF.iterrows():
                if (row.Name == row2.Name):
                    Pa = row.Path.replace(Synch_From, "")
                    
                    if (Pa2 == Pa):  
                        check = 'X'
                        break
                    
            if (check == ""):
                local_path = r'%s' % row2.Path2
                print("Removing: ", local_path)
                
                if os.path.isdir(local_path):
                    print("Rm dir")
                    local_path = local_path + r"/"
                    shutil.rmtree(local_path)
                    
                elif os.path.isfile(local_path):
                    print("Rm file")
                    os.remove(local_path)
        except:
            print("Error while running junk detection")  
    print("End of Junk_Detection\n")    

def show_progress(index, T_Count):
    global zehner
    percent = (index / T_Count)  * 100
    if (percent >= zehner): 
        zehner += 10
        percent = str(round(percent, 2))
        print("Current Progress: ", percent, "%")
        return zehner
            
#-----------------------------------------------------------------
#Start of script
#Read client partion for synch with server
Get_Data(Synch_From, WinDF, CountWin, NameWIN)
TotalCount_1 = TotalCount
print("There are ",TotalCount_1, " files to synch.")
#Read server partion for synch with client
Get_Data(Synch_To, ServerDF, CountServer, NameSe)
TotalCount_2 = TotalCount - TotalCount_1
print("In the save Folder are currently ",TotalCount_2, " files.")

WinDF.drop_duplicates()
ServerDF.drop_duplicates()

#Deletes asynch files and directorys
Junk_detection(WinDF, ServerDF)

#Check if corrosponding file and directory is destination side
# and uploade/creat new directory or files
Check_Exist(WinDF, ServerDF)

MainDF = pd.merge(WinDF, ServerDF, how="inner", on=["Name"])
MainDF.drop_duplicates()

#Modifying files that exists on both systems in the same directory
Set_Flag(MainDF)

Check_Change(MainDF)

#if you dont need logs make the following line to a comment
Create_Log(MainDF, LogDF)

print("Finished")
#End of script
#-----------------------------------------------------------------