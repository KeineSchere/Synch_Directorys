# Synch with file server
# This script synchronizes two directories or whatever you want
# Author: KeineSchere
# License: GPLv3 // https://www.gnu.org/licenses/gpl-3.0.de.html
from __future__ import print_function
import os
import pandas as pd
import time
import datetime
import shutil
from datetime import datetime
import multiprocessing

# Replace with your directory path you want to copy
Synch_From = r'D:/ExampleCopyFrom/'
# Replace with your directory path you want to copy to
Synch_To   = r'Y:/ExampleCopyTo/'
# Replace with the location you want to safe the log
# If you don't set a location the script don't generate a log
Safe_Log   = r'Y:ExampleSafeLocation/'

global ServerDF
ServerDF = pd.DataFrame(columns=['Name', 'Created2', 'Modified2', 'Path2', 'Parent2'])
NameSe   = 'ServerDF'
global WinDF
WinDF = pd.DataFrame(columns=['Name', 'Created', 'Modified', 'Path', 'Parent', 'Modded'])
NameWIN  = 'WinDF'
LogDF    = pd.DataFrame(columns=['Action', 'Time', 'Info'])
CountServer = 0
CountWin = 0
TotalCount = 0
TotalSize = 0
Total_Trash_Files = 0

global zehner
zehner = 10

global inp

def Get_Data(dir_path, DF, Name, results):
    """ Sammelt rekursiv Daten von einem Verzeichnis und seinen Unterverzeichnissen
        und speichert sie in einem DataFrame mit Fortschrittsanzeige.

    Args:
        dir_path (str): Der Pfad des zu durchsuchenden Verzeichnisses.
        DF (pd.DataFrame): Der DataFrame, in den die Daten gespeichert werden sollen.
        Name (str): Der Name des DataFrames ('ServerDF' oder 'WinDF').
        results (multiprocessing.Queue): Eine Queue zur Rückgabe des befüllten DataFrames und Statistiken.
    """
    local_df = pd.DataFrame(columns=DF.columns)
    local_total_count = 0
    local_total_size = 0
    local_total_trash_files = 0
    local_log_entries = []
    processed_count = 0

    all_items = []
    for root, _, files in os.walk(dir_path):
        for name in files:
            all_items.append(os.path.join(root, name))
        for name in os.listdir(root):
            File_Path = os.path.join(root, name)
            if os.path.isdir(File_Path) and 'DONT_SYNCH_DIR' not in File_Path and 'System Volume' not in File_Path and '$RECYCLE' not in File_Path:
                all_items.append(File_Path)

    total_items = len(all_items)
    zehner_local = 1

    for item_path in all_items:
        local_total_count += 1
        processed_count += 1

        if os.path.isfile(item_path):
            name = os.path.basename(item_path)
            root = os.path.dirname(item_path)
        elif os.path.isdir(item_path):
            name = os.path.basename(item_path)
            root = os.path.dirname(item_path)
        else:
            continue

        if 'desktop.ini' in item_path or 'System Volume' in item_path or '$RECYCLE' in item_path:
            local_total_trash_files += 1
            #print("Trash: ", item_path)
            #local_log_entries.append({'Action': 'Cancel Access', 'Time': datetime.now(), 'Info': 'Windows Trash'})

        elif 'DONT_SYNCH_DIR' in item_path:
            #print("Script found a dir that should nor get synched: ", item_path, end='\n')
            local_total_trash_files += 1
        else:
            try:
                ti_c = os.path.getctime(item_path)
                ti_m = os.path.getmtime(item_path)

                # Converting the time in seconds to a timestamp
                c_ti = time.ctime(ti_c)
                m_ti = datetime.fromtimestamp(ti_m).strftime('%Y-%m-%d %H:%M:%S')

                if os.path.isfile(item_path):
                    file_size = os.path.getsize(item_path)
                    local_total_size += file_size
                    parent_path = root

                    if Name == 'WinDF':
                        local_df.loc[len(local_df.index)] = [name, c_ti, m_ti, item_path, parent_path, '']
                    else:
                        local_df.loc[len(local_df.index)] = [name, c_ti, m_ti, item_path, parent_path]

                elif os.path.isdir(item_path):
                    parent_path = root
                    if Name == 'WinDF':
                        local_df.loc[len(local_df.index)] = [name, c_ti, m_ti, item_path, parent_path, '']
                    else:
                        local_df.loc[len(local_df.index)] = [name, c_ti, m_ti, item_path, parent_path]

                # Lokale Fortschrittsanzeige
                if total_items > 0:
                    percent = (processed_count / total_items) * 100
                    if percent >= zehner_local:
                        zehner_local += 1
                        percent_str = str(round(percent, 2))
                        print(f"\n  {Name} Progress: {percent_str} %", end='\r')

            except Exception as e:
                print(f"Error processing item '{item_path}': {e}")

    results.put((local_df, local_total_count, local_total_size, local_total_trash_files, local_log_entries))
    print(f"  {Name} Progress: 100.00 %", end='\r') # Sicherstellen, dass 100% angezeigt wird
    print() # Neue Zeile nach Abschluss

def Set_Flag(MainDF):
    print("\nStart of Set_Flag")
    for ind in range(len(MainDF)):
        # File changed
        try:
            # Check if is a file to update
            if os.path.isfile(MainDF.loc[ind, "Path"]):
                if (MainDF.loc[ind, "Modified"]) >= (MainDF.loc[ind, 'Modified2']):
                    MainDF.loc[ind, "Modded"] = 'M'
                    print("Found modded: ", MainDF.Name[ind])
        except Exception as e:
            print(f"Catched in Set_Flag: {e}")
    print("End of Set_Flag\n")

def Check_Change(MainDF):
    global LogDF
    print("\nStart of Check_Changed")
    for Modified in MainDF.index:
        try:
            if (MainDF['Modded'][Modified] == 'M'):
                # Delete old file and upload new file
                print("Try Modifying")

                Source_Path = (MainDF['Path'][Modified])
                print(Source_Path)
                Dest_Path   = (MainDF['Parent2'][Modified])
                Relative_Path = os.path.relpath(Source_Path, Synch_From)
                New_Dest_Path = os.path.join(Dest_Path, os.path.dirname(Relative_Path))

                os.makedirs(New_Dest_Path, exist_ok=True)

                new_row = {'Action': 'Updated File', 'Time': datetime.now(), 'Info': Source_Path}
                LogDF = LogDF._append(new_row, ignore_index=True)

                # Delete modified file and copy new version of the file
                Del_File = os.path.join(New_Dest_Path, MainDF['Name'][Modified])
                if os.path.exists(Del_File):
                    os.remove(Del_File)
                    print("Removed old Version")
                    shutil.copy2(Source_Path, New_Dest_Path)
                    print("Successfully uploaded new Version")
                else:
                    print(f"Warning: Old file not found at '{Del_File}'")
        except Exception as e:
            print(f"Catched in Check_Change: {e}")
            new_row = {'Action': 'Update File ERROR', 'Time': datetime.now(), 'Info': MainDF['Name'][Modified]}
            LogDF = LogDF._append(new_row, ignore_index=True)
    print("End of Check_Changed\n")

def Create_Log(MainDF, LogDF):
    print("\nStart of creating Log")
    now = datetime.now()
    current_time = now.strftime("%Y-%m-%d_%H-%M-%S")
    Safe_Path = os.path.join(Safe_Log, f"Log_{current_time}.csv")
    # Create a Log-File for the whole Dic. structure and changed files
    LogDF.to_csv(Safe_Path, index=False)
    MainDF.to_csv(Safe_Path, index=False, mode="a")
    print("End of creating Log\n")

def Check_Exist(WinDF, ServerDF):
    print("\nStart of check_exist")
    global zehner
    global LogDF
    zehner = 0
    TotalCount_1 = len(WinDF)

    for index, row in WinDF.iterrows():
        show_progress(index, TotalCount_1)

        try:
            print("Check: ", row['Path'])  # Zugriff auf die Spalte 'Path' der aktuellen Zeile
            check = ""
            Relative_Path = os.path.relpath(row['Path'], Synch_From)
            New_Server_Path = os.path.join(Synch_To, Relative_Path)

            for index2, row2 in ServerDF.iterrows():
                Server_Relative_Path = os.path.relpath(row2['Path2'], Synch_To)
                if row2['Name'] == row['Name'] and Server_Relative_Path == Relative_Path:
                    check = "X"
                    break

            # Create new file/dir
            if (check == ""):
                if os.path.isdir(row['Path']):
                    print("Creating directory: ", New_Server_Path)
                    os.makedirs(New_Server_Path, exist_ok=True)
                    new_row = {'Action': 'Created new dir', 'Time': datetime.now(), 'Info': row['Path']}
                    LogDF = LogDF._append(new_row, ignore_index=True)

                elif os.path.isfile(row['Path']):
                    print("Copying file: ", New_Server_Path)
                    os.makedirs(os.path.dirname(New_Server_Path), exist_ok=True)
                    shutil.copy2(row['Path'], New_Server_Path)
                    new_row = {'Action': 'Copied new file', 'Time': datetime.now(), 'Info': row['Path']}
                    LogDF = LogDF._append(new_row, ignore_index=True)
                else:
                    print(f"Warning: '{row['Path']}' is neither a file nor a directory.")
        except Exception as e:
            print(f"Error in Check_Exist for '{row['Path']}': {e}")
    show_progress(100, 100)
    print("\nEnd of check_exist")

def Junk_detection(WinDF, ServerDF):
    print("\nStart of Junk_Detection")
    TotalCount_2 = len(ServerDF)
    for index, row2 in ServerDF.iterrows():
        show_progress(index, TotalCount_2)

        try:
            Server_Relative_Path = os.path.relpath(row2.Path2, Synch_To)
            check = ""

            for index, row in WinDF.iterrows():
                Win_Relative_Path = os.path.relpath(row.Path, Synch_From)
                if row.Name == row2.Name and Win_Relative_Path == Server_Relative_Path:
                    check = 'X'
                    break

            if (check == ""):
                local_path = row2.Path2

                if os.path.isdir(local_path):
                    print("Removing dir: ", local_path, end='\n')
                    shutil.rmtree(local_path, ignore_errors=True)
                    print("\n")

                elif os.path.isfile(local_path):
                    print("Rm file: ", local_path, end='\n')
                    os.remove(local_path)
                    print("\n")
        except Exception as e:
            print(f"Catched exception in Junk_detection for '{row2.Path2}': {e}", end='\n')
    show_progress(100, 100)
    print("\nEnd of Junk_Detection")

def show_progress(index, T_Count):
    global zehner
    if T_Count > 0:
        percent = (index / T_Count) * 100
        if (percent >= zehner):
            zehner += 10
            percent_str = str(round(percent, 2))
            print(f"Current Progress: {percent_str} %", end='\r')
    return zehner

def Check_Input(Input):
    global inp
    inp = Input
    if (Input and Input[-1] not in ('/', '\\')):
        inp = Input + os.sep
    return inp

# -----------------------------------------------------------------
# Start of script
# Check paths and fix
Synch_From = Check_Input(Synch_From)
Synch_To = Check_Input(Synch_To)
Safe_Log = Check_Input(Safe_Log)

print("Script started, depending on the size to synch this could take a while!")

# Use multiprocessing to get data from both directories concurrently
if __name__ == '__main__':
    manager = multiprocessing.Manager()
    results_queue = manager.Queue()
    processes = []

    p1 = multiprocessing.Process(target=Get_Data, args=(Synch_From, pd.DataFrame(columns=['Name', 'Created', 'Modified', 'Path', 'Parent', 'Modded']), NameWIN, results_queue))
    processes.append(p1)
    p1.start()

    p2 = multiprocessing.Process(target=Get_Data, args=(Synch_To, pd.DataFrame(columns=['Name', 'Created2', 'Modified2', 'Path2', 'Parent2']), NameSe, results_queue))
    processes.append(p2)
    p2.start()

    result_win = results_queue.get()
    result_server = results_queue.get()

    WinDF, total_count_1, total_size_1, total_trash_files_1, log_entries_win = result_win
    ServerDF, total_count_2, total_size_2, total_trash_files_2, log_entries_server = result_server

    TotalCount_1 = total_count_1 - total_trash_files_1
    TotalSize_1 = round(((total_size_1 / 1024) / 1024), 2)
    LogDF = LogDF._append(log_entries_win, ignore_index=True)
    print(f"There are {TotalCount_1} files to synch. Which contains {TotalSize_1} MB of data.")

    TotalCount_2 = total_count_2 - total_trash_files_2
    TotalSize_2 = round(((total_size_2 / 1024) / 1024), 2)
    LogDF = LogDF._append(log_entries_server, ignore_index=True)
    print(f"In the save Folder are currently {TotalCount_2} files. Which contains {TotalSize_2} MB of data.")

    for p in processes:
        p.join()

    WinDF.drop_duplicates(inplace=True)
    ServerDF.drop_duplicates(inplace=True)

    # Deletes asynch files and directorys
    Junk_detection(WinDF, ServerDF)

    # Check if corresponding file and directory is destination side
    # and upload/create new directory or files
    Check_Exist(WinDF, ServerDF) # Hier sollten WinDF und ServerDF nun die befüllten DataFrames sein

    MainDF = pd.merge(WinDF, ServerDF, how="inner", on=["Name"])
    MainDF.drop_duplicates(inplace=True)

    # Modifying files that exists on both systems in the same directory
    Set_Flag(MainDF)

    Check_Change(MainDF)

    # Creates the log
    if Safe_Log:
        Create_Log(MainDF, LogDF)

    print("Finished, press ENTER to close the Script")
    input_s = input()