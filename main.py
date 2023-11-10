import os
import pandas as pd
import requests
import time
from os import walk
from multiprocessing import Pool, cpu_count
import sys
from glob import glob
# from dask.distributed import Client
# from dask.distributed import wait

# import a list of files from https://apps.nationalmap.gov/downloader/
# Elevation Source Data (3DEP) - Lidar, IfSAR
# Create a shopping cart and save as a CSV file
# Column R should have the web address of the file it should look something like this
# "S_Lidar_Point_Cloud_NJ_SdL5_2014_LAS_https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/USG2015/laz/18TWK610835.laz"
# Point the software to the location of the file

ShoppingCart = "/media/glen/occum/USGS_Shopping_Cart/"
LIDAR_directory = "/media/glen/occum/Lidar/"
LIDAR_XML = "/media/glen/occum/Lidar_XML/"


# LIDAR_directory = "/home/glen/TestData/"
# LIDAR_XML = "/home/glen/TestData/"


def Multiprocessing_Download_laz(args):
    global e
    URL, file_name = args[0], args[1]
    try:
        r = requests.get(URL, allow_redirects=True)
    except IOError as e:
        print("An error occurred:", e)
        time.sleep(30)  # Sleep for 30 seconds
        r = requests.get(URL, allow_redirects=True)
    if r.reason == 'Service Unavailable':
        print("URL=" + URL + ' Service Unavailable')
    elif r.status_code == 200:
        open(file_name, 'wb').write(r.content)
        print("File " + file_name + " added to REPO")
    elif r.status_code == 404:
        print("URL=" + URL + ' File not found Error 404')
    else:
        print("An error occurred:", e)
    return


def Download_XML(URL_xml, file_name):

    global e
    try:
        r = requests.get(URL_xml, allow_redirects=True)
    except IOError as e:
        print("An error occurred:", e)
        time.sleep(30)  # Sleep for 30 seconds
        r = requests.get(URL_xml, allow_redirects=True)
    if r.reason == 'Service Unavailable':
        print("URL=" + URL_xml + ' Service Unavailable')
    elif r.status_code == 200:
        open(file_name, 'wb').write(r.content)
        print("File " + file_name + " added to REPO")
    elif r.status_code == 404:
        temp_URL = URL_xml.replace(".xml", "_meta.xml")
        print("Error 404 switching to _meta.xml")
        try:
            r = requests.get(temp_URL, allow_redirects=True)
        except IOError as e:
            print("An error occurred:", e)
        if r.status_code == 200:
            open(file_name, 'wb').write(r.content)
            print("File " + file_name + " added to REPO")
        elif r.status_code == 404:
            print("second try failed review URL link")
        else:
            print("An error occurred:", e)
    return


# changing url link in Parallel seems to have issues don't use on small XML files
def Multiprocessing_Download_XML(args):
    global e
    URL, file_name = args[0], args[1]
    try:
        r = requests.get(URL, allow_redirects=True)
    except IOError as e:
        print("An error occurred:", e)
        time.sleep(30)  # Sleep for 30 seconds
        r = requests.get(URL, allow_redirects=True)
    if r.reason == 'Service Unavailable':
        print("URL=" + URL + ' Service Unavailable')
    elif r.status_code == 200:
        open(file_name, 'wb').write(r.content)
        print("File " + file_name + " added to REPO")
    elif r.status_code == 404:
        URL = URL.replace(".xml", "_meta.xml")
        print("Error 404 switching to _meta.xml")
        try:
            r = requests.get(URL, allow_redirects=True)
        except IOError as e:
            print("An error occurred:", e)
        if r.status_code == 200:
            open(file_name, 'wb').write(r.content)
            print("File " + file_name + " added to REPO")
        elif r.status_code == 404:
            print("second try failed review URL link")
        else:
            print("An error occurred:", e)

    return


if __name__ == '__main__':

    dir_list_ShoppingCart = os.listdir(ShoppingCart)
    MasterList = pd.DataFrame(columns=['Name', 'XML_Link', 'Catalog', 'File_Link', 'LAZ_File_Name', 'XML_File_Name', 'Last_Update'])

    LIDAR_Shopping_Cart_DF = pd.DataFrame()
    for file in dir_list_ShoppingCart:
        if ".csv" in file:
            LIDAR_Shopping_Cart_DF = pd.read_csv(ShoppingCart + file, header=None)
            LIDAR_Shopping_Cart_DF = LIDAR_Shopping_Cart_DF[[0, 4, 6, 14, 24]]
            LIDAR_Shopping_Cart_DF.columns = ['Name', 'XML_Link', 'Catalog', 'File_Link', 'Last_Update']
            MasterList = pd.concat([MasterList, LIDAR_Shopping_Cart_DF])

    print("Number of Files currently in Shopping Cart = {}".format(len(MasterList.index)))

    # Step 1 get a list of Lidar files we have
    LAZ_files_in_LIDAR_REPO = glob(LIDAR_directory + '/**/*.laz', recursive=True)
    temp1 = [os.path.basename(list_item) for list_item in LAZ_files_in_LIDAR_REPO]
    list_of_files_names_in_LIDAR_REPO = [os.path.splitext(list_item)[0] for list_item in temp1]
    list_of_files_names_in_LIDAR_REPO = pd.Series(list_of_files_names_in_LIDAR_REPO).drop_duplicates().tolist()
    print("Number of Files currently in LIDAR REPO = {}".format(len(list_of_files_names_in_LIDAR_REPO)))

    #"USGS Lidar Point Cloud CA_SANFRANBAY_2004 000010.laz"

    #if "USGS Lidar Point Cloud CA_SANFRANBAY_2004 000010" in list_of_files_names_in_LIDAR_REPO:
    #    print("exist")
    #else:
    #   print("not exist")



    # Step 3 compare list to create our dataframe of needed downloads setting Column Download to True
    # LAZ files
    MasterList.reset_index(drop=True, inplace=True)
    MasterList['Download_LAZ'] = None
    for index in MasterList.index:
        name = MasterList.iloc[index]['Name']
        if name in list_of_files_names_in_LIDAR_REPO:
            MasterList.at[index, "Download_LAZ"] = 'False'
        else:
            MasterList.at[index, "Download_LAZ"] = 'True'

    # check how many file need to be downloaded
    test = MasterList.loc[MasterList['Download_LAZ'] == 'True']

    # get a list of all file in XML REPO
    XML_files_in_LIDAR_REPO = glob(LIDAR_XML + '/**/*.xml', recursive=True)
    temp1 = [os.path.basename(list_item) for list_item in XML_files_in_LIDAR_REPO]
    list_of_files_names_in_XML_REPO = [os.path.splitext(list_item)[0] for list_item in temp1]
    list_of_files_names_in_XML_REPO = pd.Series(list_of_files_names_in_XML_REPO).drop_duplicates().tolist()
    print("Number of Files currently in XML REPO = {}".format(len(list_of_files_names_in_XML_REPO)))

    # build a list of all files needed by combine LAZ and XML to look for missing
    CombinedListOfNeededFiles = list_of_files_names_in_LIDAR_REPO + list_of_files_names_in_XML_REPO
    CombinedListOfNeededFiles = pd.Series(CombinedListOfNeededFiles).drop_duplicates().tolist()

    #if "USGS Lidar Point Cloud CA_SANFRANBAY_2004 000010" in list_of_files_names_in_XML_REPO:
    #    print("exist")
    #else:
    #    print("not exist")

    MasterList['Download_XML'] = None
    for index in MasterList.index:
        if MasterList.iloc[index]['Name'] in list_of_files_names_in_XML_REPO:
            MasterList.at[index, "Download_XML"] = 'False'
        else:
            MasterList.at[index, "Download_XML"] = 'True'

            # we need to fix the XML links by replacing LAZ with Metadata
            # format of request
            # https://www.sciencebase.gov/catalog/item/download/64390eb1d34ee8d4ade0af15?format = json"
            # https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/IL_4_County_QL1_LiDAR_2016_B16/IL_4County_Kane_2018/metadata/USGS_LPC_IL_4_County_QL1_LiDAR_2016_B16_LAS_00008175.xml
            # https://rockyweb.usgs.gov/vdelivery/Datasets/Staged/Elevation/LPC/Projects/IL_4_County_QL1_LiDAR_2016_B16/IL_4County_Kane_2018/LAZ/USGS_LPC_IL_4_County_QL1_LiDAR_2016_B16_LAS_00759725.laz

    for index in MasterList.index:
        LAZ_Link = MasterList.iloc[index]['File_Link']
        XML_Link = ""
        if "/LAZ/" in LAZ_Link:  # upper lower case problems on linux
            XML_Link = LAZ_Link.replace("/LAZ/", "/metadata/")
        if "/laz/" in LAZ_Link:
            XML_Link = LAZ_Link.replace("/laz/", "/metadata/")
        XML_Link = XML_Link.replace(".laz", ".xml")
        MasterList.at[index, "XML_Link"] = XML_Link
        MasterList.at[index, "LAZ_File_Name"] = LIDAR_directory + MasterList.iloc[index]['Name'] + ".laz"
        MasterList.at[index, "XML_File_Name"] = LIDAR_XML + MasterList.iloc[index]['Name'] + ".xml"

    LAZ_ListOfSitesToDownload = MasterList.loc[MasterList['Download_LAZ'] == 'True']
    LAZ_ListOfSitesToDownload.reset_index(drop=True, inplace=True)

    URL_laz = list(LAZ_ListOfSitesToDownload["File_Link"])
    FileName = list(LAZ_ListOfSitesToDownload["LAZ_File_Name"])
    inputs = zip(URL_laz, FileName)
    print(cpu_count())
    pool = Pool(cpu_count() - 1)
    pool.map(Multiprocessing_Download_laz, inputs)
    pool.close()


    XML_ListOfSitesToDownload = MasterList.loc[MasterList['Download_XML'] == 'True']
    XML_ListOfSitesToDownload.reset_index(drop=True, inplace=True)

    for index in XML_ListOfSitesToDownload.index:
        URL_XML = XML_ListOfSitesToDownload.iloc[index]["XML_Link"]
        FileName = XML_ListOfSitesToDownload.iloc[index]["XML_File_Name"]
        Download_XML(URL_XML, FileName)

    #URL_XML = list(XML_ListOfSitesToDownload["XML_Link"])
    #FileName = list(XML_ListOfSitesToDownload["XML_File_Name"])
    #inputs = zip(URL_XML, FileName)
    #pool = Pool(20)
    #pool.map(Multiprocessing_Download_XML, inputs)
    #pool.close()

    #list_of_files_in_LIDAR_REPO list_of_files_in_XML_REPO

    #list_of_files_names_in_LIDAR_REPO + list_of_files_names_in_XML_REPO

    #print("Missing values in list_of_files_in_XML_REPO:", (set(list_of_files_in_LIDAR_REPO).difference(list_of_files_in_XML_REPO)))
    missingXML = set(list_of_files_names_in_LIDAR_REPO).difference(list_of_files_names_in_XML_REPO)
    #print("Additional values in list_of_files_in_XML_REPO:", (set(list_of_files_in_XML_REPO).difference(list_of_files_in_LIDAR_REPO)))

    # prints the missing and additional elements in list1
    #print("Missing values in list_of_files_in_LIDAR_REPO:", (set(list_of_files_in_XML_REPO).difference(list_of_files_in_LIDAR_REPO)))
    missingLAZ = set(list_of_files_names_in_XML_REPO).difference(list_of_files_names_in_LIDAR_REPO)
    #print("Additional values in list_of_files_in_LIDAR_REPO:", (set(list_of_files_in_LIDAR_REPO).difference(list_of_files_in_XML_REPO)))



    print("done")
