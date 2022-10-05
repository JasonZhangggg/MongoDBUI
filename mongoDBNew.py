import tabulate
from pymongo import MongoClient
import pandas as pd
import csv
import re

# cluster = "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false"

# Link to the MongoDB cluster
cluster = "mongodb://table_extraction:vlldc3JRnDG2DvW3@cluster0-shard-00-00.afvrd.mongodb.net:27017,cluster0-shard-00-01.afvrd.mongodb.net:27017,cluster0-shard-00-02.afvrd.mongodb.net:27017/myFirstDatabase?ssl=true&replicaSet=atlas-12bs5c-shard-0&authSource=admin&retryWrites=true&w=majority"

client = MongoClient(cluster)

# Get the database
db = client.data

# Get all of the clusters
db_Test = db.data
db_Melt_Pool = db.Melt_Pool
db_Refractory_Alloys = db.Refractory_Alloys
db_Super_Alloys = db.Super_Alloys
db_Mechanical_Properties = db.Mechanical_Properties
db_HEA_Creep = db.ElsevierHEACreepPDFs
db_Super_Alloys_Creep = db.ElsevierSuperAlloyPDFs

# Add the clusters into a list
collection_list = {
    'All': [db_Test, db_Melt_Pool, db_Refractory_Alloys, db_Super_Alloys, db_Mechanical_Properties, db_HEA_Creep,
            db_Super_Alloys_Creep], 'Test': [db_Test], 'Mechanical Properties': [db_Mechanical_Properties],
    'Melt Pool': [db_Melt_Pool], 'Refractory Alloys': [db_Refractory_Alloys], 'Super Alloys': [db_Super_Alloys],
    'Hea Creep': [db_HEA_Creep], 'Super Alloys Creep': [db_Super_Alloys_Creep]}
value = ""
searchType = ""
df_dict = []
lastSearch = ""

doi_dict = {}
reader = csv.reader(open('doi.csv', 'r', encoding="utf8"))
for k, v in reader:
    doi_dict[k] = v


# Bold text formatting
class font:
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'


# Merge title with tagged title
def merge(list1, list2):
    if isinstance(list1, str):
        return list1
    merged_list = ["(" + list1[i] + ", " + list2[i] + ")" for i in range(0, len(list1))]
    return merged_list


def mergeTables(df_dict):
    b = len(df_dict)
    cbdf_list = {}
    if len(df_dict) > 0:
        df = df_dict[0]
        df = df.reindex(sorted(df.columns), axis=1)
        # saving inside cbdf_list dictionary as lists
        names = list(df.columns)
        for j in range(len(names)):
            cbdf_list[names[j]] = df[names[j]].tolist()

        for i in range(b - 1):
            i = i + 1
            df = df_dict[i]
            df = df.reindex(sorted(df.columns), axis=1)
            names = list(df.columns)
            temp = {}
            for j in range(len(names)):
                new = True
                for k, v in cbdf_list.items():
                    if names[j].lower().strip() == k.lower().strip():
                        cbdf_list[k].extend(df[names[j]].tolist())
                        new = False
                if new:
                    temp[names[j]] = [None] * len(v)
                    temp[names[j]].extend(df[names[j]].tolist())
            cbdf_list.update(temp)
            lengths = [len(v) for v in cbdf_list.values()]
            max_L = max(lengths)
            for k, v in cbdf_list.items():
                if len(v) < max_L:
                    diff = max_L - len(v)
                    emp_L = [None] * diff
                    cbdf_list[k].extend(emp_L)
    return cbdf_list


# Print out the "cursor"
def getPDFs(cursor, type, search_elem, collection):
    global value
    global searchType
    global df_dict
    search_elem = search_elem.lower()
    last_title = ""
    print("Searches found: " + str(len(list(cursor.clone()))))
    print("____________________________________________ \n \n ")

    table_list = set()
    # table_count = 0
    for document in cursor:
        flag = False
        if searchType == 'off':
            if type == "name" or type == "value" or type == "both":
                for elem in document['body']:
                    if type == "name" or type == "value":
                        if search_elem in elem[type].lower().split(" "):
                            flag = True
                    else:
                        if search_elem in elem['name'].lower().split(" ") or search_elem in elem[
                            'value'].lower().split():
                            flag = True
            else:
                if search_elem in document[type].lower().split(" "):
                    flag = True
        else:
            flag = True
        if flag:
            table_list.add(document['title'])
    return display(sorted(table_list), collection)


def display(table_list, collection):
    output_list = []
    for title in table_list:
        id_list = []
        output_dict = {'flagged': False, 'pdf_title': '', 'title': '', 'tagged_title': '', 'doi': '', 'table': '',
                       'table_csv': '',
                       'id': []}
        table_dict = {}
        rows = []
        table_csv = []
        if title != '':
            doc_list = searchTitle(title, collection)
            for document in doc_list:
                for elem in document['body']:
                    table_dict[elem['name']] = elem['value']
                if not output_dict['flagged']:
                    output_dict['flagged'] = document['Flag']
                id_list.append(document['_id'])
                rows.append(list(table_dict.values()))
                output_dict['pdf_title'] = document['pdf_title']
                output_dict['title'] = document['title']
                output_dict['tagged_title'] = ", ".join(
                    merge(document['tagged_title'], document['tags']))
                if document['pdf_title'] in doi_dict:
                    output_dict['doi'] = doi_dict[document['pdf_title']]
                else:
                    output_dict['doi'] = "none"

            header_format = [elem for elem in table_dict.keys()]
            header = list(table_dict.keys())
            table_csv.append(header)
            for row in rows:
                table_csv.append(row)
            output_dict['table_csv'] = table_csv
            # loop through the table headers
            for i in range(len(header)):
                col = []
                # loop through each value in a column
                for row in rows:
                    # if the current row is missing elements fill the column array with an empty element
                    # i>=len(row) means current row does not have a val for column i
                    if i < len(row):
                        col.append(row[i])
                    else:
                        col.append('')
                table_dict[header[i]] = col
            df_dict.append(pd.DataFrame(table_dict))
            output_dict['table'] = tabulate.tabulate(rows, header_format)
            output_dict['id'] = id_list

            output_list.append(output_dict)
    return output_list


# Search only in the header. Return cursor with elements containing the desired value in the header
def searchHeader(elem, db):
    cursor = db.find({
        'body': {'$elemMatch': {'name': {'$regex': re.compile(elem, re.IGNORECASE)}}}
    })
    return cursor


# Search only in the Value. Return cursor with elements containing the desired value in the table
def searchValue(elem, db):
    cursor = db.find({
        'body': {'$elemMatch': {'value': {'$regex': re.compile(elem, re.IGNORECASE)}}}
    })
    return cursor


# Search both the header and value. Return cursor with elements that contain the desired header or value in the table
def searchHeaderValue(elem, db):
    cursor = db.find({
        '$or': [
            {'body': {'$elemMatch': {'value': {'$regex': re.compile(elem, re.IGNORECASE)}}}},
            {'body': {'$elemMatch': {'name': {'$regex': re.compile(elem, re.IGNORECASE)}}}}
        ]
    })
    return cursor


# Same as above but with the PDF Title
def searchPDFTitle(elem, db):
    cursor = db.find({'pdf_title': {'$regex': re.compile(elem, re.IGNORECASE)}})
    return cursor


# Same as above but with the table title
def searchTitle(elem, db):
    # cursor = db.find({'title': {'$regex': re.compile(elem, re.IGNORECASE)}})
    cursor = db.find({'title': {'$regex': re.compile(re.escape(elem), re.IGNORECASE)}})
    return cursor


def searchExactTitle(elem, db):
    cursor = db.find({'title': elem})
    return cursor


# Same as above but with table title tags
def searchTags(elem, db):
    cursor = db.find({
        'tags': {'$elemMatch': {'$regex': re.compile(elem, re.IGNORECASE)}}
    })
    return cursor


running = True


def search(searchParam, newSearchType, searchDatabase, value):
    global lastSearch
    global searchType
    # Prompt user
    lastSearch = value + searchParam + newSearchType + searchDatabase
    searchType = newSearchType

    collections = collection_list[searchDatabase]
    for collection in collections:
        if searchParam == 'Table header':
            return getPDFs(searchHeader(value, collection), "name", value, collection)
        elif searchParam == 'Value':
            return getPDFs(searchValue(value, collection), "value", value, collection)
        elif searchParam == 'Header and Value':
            return getPDFs(searchHeaderValue(value, collection), "both", value, collection)
        elif searchParam == 'PDF Title':
            return getPDFs(searchPDFTitle(value, collection), "pdf_title", value, collection)
        elif searchParam == 'Table Title':
            return getPDFs(searchTitle(value, collection), "title", value, collection)
        elif searchParam == 'Tagged Title':
            return getPDFs(searchTags(value, collection), "tags", value, collection)


def csvOutput():
    global df_dict
    global lastSearch
    cbdf_list = mergeTables(df_dict)
    cbdf = pd.DataFrame.from_dict(cbdf_list)
    cbdf.to_csv(lastSearch + '.csv', index=False)
    df_dict = []
    return lastSearch


def update_db(id, aspect, val):
    for m_db in collection_list['All']:
        m_db.update_one({
            '_id': id
        }, {
            '$set': {
                aspect: val
            }
        }, upsert=False)


def edit_table(ids, tables):
    for m_db in collection_list['All']:
        for id in ids:
            m_db.delete_one({
                '_id': id
            })

        m_db.insert_many(tables)
