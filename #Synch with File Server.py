#Synch with File Server
#This script synchronizes two partions or directorys
#Author: Fabian Weiershausen
#License: GPLv3 // https://www.gnu.org/licenses/gpl-3.0.de.html
from IPython.display import display
import os
import pandas as pd
import time
import shutil
from datetime import datetime

ServerDF = pd.DataFrame(columns=['Name', 'Created2', 'Modified2', 'Paath2', 'Parent2'])
NameSe = 'ServerDF'
WinDF = pd.DataFrame(columns=['Name', 'Created', 'Modified', 'Path', 'Parent', 'Modded'])
NameWIN = 'WinDF'
LogDF = pd.DataFrame(columns=['Action, Time, Info'])
CountServer = 0
CountWin = 0
Safe_Log = r'X:/Synch/Log/'

def Get_Data(dir_path, DF, count, Name):
       
    for path in os.listdir(dir_path):
           
        print(dir_path)

        count += 1

        File_Path = dir_path + path 
                
        if 'desktop.ini' in File_Path or 'System Volume' in File_Path or '$RECYCLE' in File_Path:
            LogDF._append({'Action':'Cancel Access', 
                            'Time': time, 
                            'Info': 'Windows Trash'}, ignore_index=True)
            print('Fick dich Windows!!!')
                    
        else: 
        
            ti_c = os.path.getctime(File_Path)
            ti_m = os.path.getmtime(File_Path)

            # Converting the time in seconds to a timestamp
            c_ti = time.ctime(ti_c)
            m_ti = time.ctime(ti_m)
        
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
        
                display(DF)
                print(f"{count} Objects were Counted in Directory")

def Set_Flag(MainDF):
    for ind in range(len(MainDF)):
        # File Changed 
        try:
            #Chech if Obj is a Directory if so, no need to replace 
            if os.path.isfile(MainDF.loc[ind, "Path"]):
                if (MainDF.loc[ind, "Modified"]) != (MainDF.loc[ind, 'Modified2']):
                    print('checkM')
                    MainDF.loc[ind, "Modded"] = 'M'
                    print (MainDF.Modded[ind])
            
                # New Path
                elif (MainDF.loc[ind, "Path"]) != (MainDF.loc[ind, 'Path2']):
                    print('checkP')
                    MainDF.loc[ind, "Modded"] = 'P'
                    print (MainDF.Modded[ind])
            
                # New File
                elif (MainDF.loc[ind, "Name"]) != (MainDF.loc[ind, 'Name2']):
                    print('checkNF')
                    MainDF.loc[ind, "Modded"] = 'NF'
                    print (MainDF.Modded[ind])
            else:
                print('Not a File')
                
        except:
            print("Catched")        

def Check_Change(MainDF):
     for Modified in MainDF.index:
        try:
            #Whole Path for Controll that it is really a file and not a Directory 
            if (MainDF['Modded'][Modified] == 'M'):
                #Change File with new Directory or Del File and replace with new Version
                print("Try Modifying")

                Source_Path = (MainDF['Path'][Modified])
                print(Source_Path)
                Dest_Path   = (MainDF['Parent2'][Modified])
                
                #LogDF._append({'Action':'Updated File', 
                #               'Time': time, 
                #               'Info': (MainDF['Path'][Modified])}, ignore_index=True)
                   
                #Delte modified file and copy new version of the file
                print("test")
                Del_File = os.path.join(MainDF['Parent2'][Modified],MainDF['Name'][Modified])
                os.remove(Del_File)
                print ("removed")
                shutil.copy(str(Source_Path), str(Dest_Path))
            
        except:
            print("Catched")
            LogDF._append({'Action':'Update File ERROR', 
                            'Time': time, 
                            'Info': (WinDF['Name'][Modified])}, ignore_index=True)
                
def Create_Log(MainDF, LogDF):
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d_%H-%M-%S")
    Safe_Path = Safe_Log + "Log" + str(current_time)
    #Create a Log-File for the whole Dic. structure and changed files
    LogDF.to_csv(Safe_Path, index=False)
    MainDF.to_csv(Safe_Path, index=False, mode="a")
    #Creates a txt file / Not recommended just use a csv!
    #with open(str(Safe_Path), "x") as Log:
    #    LogDF = LogDF.to_string(header=False, index=False)
    #    WinDF = WinDF.to_string(header=True, index=True)
    #    Write_Log = "Win:" ,WinDF, "Server:", ServerDF
    #    Log.write(str(Write_Log))

#Read client partion for synch with server
Get_Data(r'D:/', WinDF, CountWin, NameWIN)
#Read server partion for synch with client
Get_Data(r'X:/Dateien/', ServerDF, CountServer, NameSe)

WinDF.drop_duplicates()
ServerDF.drop_duplicates()

MainDF = pd.merge(WinDF, ServerDF, how="inner", on=["Name"])
MainDF.drop_duplicates()
display(MainDF)

Set_Flag(MainDF)

Check_Change(MainDF)

Create_Log(MainDF, LogDF)

print("Finished")