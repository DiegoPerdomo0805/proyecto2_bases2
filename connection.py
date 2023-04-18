import fastavro
import glob
import json
import avro
import avro.datafile
import avro.io
import datetime
import os

def check_table(table):
    if table == '':
        return False
    else:
        #return True
        found = False
        tables = glob.glob('./tables/*.avro')
        for e in tables:
            with open(e, 'rb') as f:
                reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
                schema = reader.meta
                schema_str = schema['avro.schema'].decode('utf-8')
                schema_dict = json.loads(schema_str)
                table_name = schema_dict['name']
                if table_name == table:
                    found = True
                    break
        return found
        
    

def create_table(name, c_families):
    # crear esquema con nombre de tabla 
    schema2 = {
        "type": "record",
        "name": name,
        "enable": True,
        "fields": [
            {
                "name": "rowkey",
                "type": "int",
                "default": 0
            }
        ]
    }
    # Write the schema to a file
    for e in c_families:
        schema2["fields"].append({"name": e, "type": {
            "type": "record",
            "name": e,
            "fields": [
                {
                    "name": "__void__",
                    "type": "string",
                    "default": "__void__"
                }
            ]
        }})

    record = {
        "rowkey": 1
    }

    # resgitros vacios que luego se llenaran con put 
    # y no se registran en la metadata
    for e in c_families:
        record[e] = {"__void__":"__void__"}

    path = './tables/'+name+'.avro'

    with open(path, "wb") as avro_file:
        fastavro.writer(avro_file, schema2, [record])
    
    
    return True

    

def dropTable(table):
    path = './tables/'+table+'.avro'
    os.remove(path)
    return "Table "+ table +" deleted"


def dropAllTables():
    tables = glob.glob('./tables/*.avro')
    for e in tables:
        os.remove(e)
    return "Se eliminaron todas las tablas: " + str(tables)



def listTable():
    tables = glob.glob('./tables/*.avro')
    tables_list = []
    for e in tables:
        with open(e, 'rb') as f:
            reader = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            schema = reader.meta
            schema_str = schema['avro.schema'].decode('utf-8')
            schema_dict = json.loads(schema_str)
            table_name = schema_dict['name']
            tables_list.append(table_name)
    return tables_list


def enable(table):
    path = './tables/'+table+'.avro'
    with open(path, 'rb') as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
        schema_dict = schema
        schema_dict['enable'] = True
        record = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        rs = []
        for e in record:
            rs.append(e)
    with open(path, "wb") as avro_file:
        fastavro.writer(avro_file, schema, rs)
    return "Table "+ table +" enabled"

def disable(table):
    path = './tables/'+table+'.avro'
    with open(path, 'rb') as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
        schema_dict = schema
        schema_dict['enable'] = False
        record = avro.datafile.DataFileReader(f, avro.io.DatumReader())
        rs = []
        for e in record:
            rs.append(e)
    with open(path, "wb") as avro_file:
        fastavro.writer(avro_file, schema, rs)
    return "Table "+ table +" disabled"

def isEnabled(table):
    path = './tables/'+table+'.avro'
    with open(path, 'rb') as f:
        reader = fastavro.reader(f)
        schema = reader.writer_schema
        schema_dict = schema
        return schema_dict['enable']



def alterAdd(table, old, new):
    path = './tables/'+table+'.avro'
    if isEnabled(table):
        with open(path, 'rb') as f:
            reader = fastavro.reader(f)
            schema = reader.writer_schema
            schema_dict = schema

            cf_exists = False

            for e in schema_dict['fields']:
                if e['name'] == old:
                    cf_exists = True
                    break

            if not cf_exists:
                return "Column Family {"+ old +"} does not exist"


            for e in schema_dict['fields']:
                if e['name'] == old:
                    #e1['type']
                    e['type']['name'] = new
                    e['name'] = new
                    break
            record = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            rs = []
            for e in record:
                rs.append(e)
            #print(' * ',rs)
            #print(' * ',schema_dict)

            for e in rs:
                for j in e:
                    if j == old:
                        #print(' $$ ',e[old])
                        e[new] = e[old]
                        e.pop(old, None)
                        break

            #print(' * ',rs)
        with open(path, "wb") as avro_file:
            fastavro.writer(avro_file, schema, rs)
        return "Column Family {"+ old +"} renamed to {"+ new + "}"
    else:
        return "Table is disabled"
    

def alterDrop(table, cf):
    path = './tables/'+table+'.avro'
    if isEnabled(table):
        with open(path, 'rb') as f:
            reader = fastavro.reader(f)
            schema = reader.writer_schema
            schema_dict = schema

            cf_exists = False
            for e in schema_dict['fields']:
                if e['name'] == cf:
                    cf_exists = True
                    break

            if not cf_exists:
                return "Column Family {"+ cf +"} does not exist"


            for e in schema_dict['fields']:
                if e['name'] == cf:
                    schema_dict['fields'].remove(e)
                    break
            record = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            rs = []
            for e in record:
                rs.append(e)
            for e in rs:
                for j in e:
                    if j == cf:
                        e.pop(cf, None)
                        break
            
            #print(' * ',rs)
            #print(' * ',schema_dict)

        with open(path, "wb") as avro_file:
            fastavro.writer(avro_file, schema, rs)
        return "Column Family "+ cf +" deleted"
    else:
        return "Table is disabled"


# ////////////////////////////////////////// DML //////////////////////////////////////////

def putTable(table, rowkey, cf, column, data):
    if isEnabled(table):
        path = './tables/'+table+'.avro'
        registros = []
        with open(path, 'rb') as avro_file:
            reader = fastavro.reader(avro_file)
            reader2 = avro.datafile.DataFileReader(avro_file, avro.io.DatumReader())
            for record in reader2:
                registros.append(record)
            schema = reader.writer_schema
            schema_dict = schema

        cf_exists = False

        for e in schema_dict['fields']:
            if e['name'] == cf:
                cf_exists = True
                break
            
        if cf_exists:

            data_type, data = TypeOf(data)

            # check if null column exists
            for e in schema_dict['fields']:
                if e['name'] == cf:
                    for f in e['type']['fields']:
                        # if it exists delete it, since it is not needed anymore
                        if f['name'] == '__void__':
                            i = e['type']['fields'].index(f)
                            e['type']['fields'].pop(i)
                            break
                    break

            # add values that already exist


            # update or add new value

            row_exists = False

            #print(schema_dict)

            for e in registros:
                if e['rowkey'] == rowkey:
                    e[cf][column] = data
                    row_exists = True
                    break

            #print()
            #print(registros)
            #print(schema_dict['fields'])
            if not row_exists:
                new_row = {
                    "rowkey": rowkey
                }
                for e in schema_dict['fields']:
                    #print(e['name'])
                    if e['name'] == cf:
                        new_row[cf] = {}
                        for f in e['type']['fields']:
                            new_row[cf][f['name']] = f['default']
                        new_row[cf][column] = data
                    else:
                        #print(e['name'])
                        if e['name'] != 'rowkey':
                            new_row[e['name']] = {}
                            for f in e['type']['fields']:
                                new_row[e['name']][f['name']] = f['default']
                #print('  +  ',new_row)
                registros.append(new_row)
                    

            # update the schema
            col_exists = False
            correct_type = False
            for e in schema_dict['fields']:
                if e['name'] == cf:
                    for f in e['type']['fields']:
                        if f['name'] == column:
                            # if it exists update it
                            col_exists = True
                            if f['type'] == data_type:
                                correct_type = True
                            break

                    if not col_exists:
                        # if it does not exist add it
                        e['type']['fields'].append({"name": column, "type": data_type, "default": createDefault(data_type)})
                        correct_type = True

            if not correct_type:
                return "Incorrect data type"
            else:
                time_stamp = datetime.datetime.now()

                #print(' * ',schema_dict)
                #print(' * ',registros)

                with open(path, "wb") as avro_file:
                    fastavro.writer(avro_file, schema_dict, registros)
                return "Valor insertado"
        else:
            #print('Column family does not exist')
            return "Column family does not exist"
    else:
        #print('Table is disabled')
        return "Table is disabled"


def getTable(table, rowkey):
    path = './tables/'+table+'.avro'
    if isEnabled(table):
        with open(path, 'rb') as f:
            record = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            for e in record:
                if e['rowkey'] == rowkey:
                    return e
            return "Rowkey does not exist"
    
def deleteAllTable(table, rowkey):
    path = './tables/'+table+'.avro'
    if isEnabled(table):
        with open(path, 'rb') as f:
            reader = fastavro.reader(f)
            reader2 = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            schema = reader.writer_schema
            schema_dict = schema
            registros = []
            for e in reader2:
                registros.append(e)
            for e in registros:
                if e['rowkey'] == rowkey:
                    registros.remove(e)
                    break
            with open(path, "wb") as avro_file:
                fastavro.writer(avro_file, schema_dict, registros)
            return "Rowkey deleted"
    else:
        return "Table is disabled"


def deleteTable(table, rowkey, cf, column):
    path = './tables/'+table+'.avro'
    if isEnabled(table):
        with open(path, 'rb') as f:
            reader = fastavro.reader(f)
            reader2 = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            schema = reader.writer_schema
            schema_dict = schema
            registros = []
            for e in reader2:
                registros.append(e)

            defaultValue = None

            for e in schema_dict['fields']:
                if e['name'] == cf:
                    for f in e['type']['fields']:
                        if f['name'] == column:
                            defaultValue = f['default']
                            break
            
            for e in registros:
                if e['rowkey'] == rowkey:
                    if cf in e:
                        if column in e[cf]:
                            e[cf][column] = defaultValue
                            break
                        else:
                            return "Column does not exist"
                    else:
                        return "Column family does not exist"
            with open(path, "wb") as avro_file:
                fastavro.writer(avro_file, schema_dict, registros)
            return "Cell deleted"
    else:
        return "Table is disabled"
    
def countTable(table):
    # count the number of rows in a table
    path = './tables/'+table+'.avro'
    if isEnabled(table):
        with open(path, 'rb') as f:
            reader = fastavro.reader(f)
            reader2 = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            schema = reader.writer_schema
            schema_dict = schema
            registros = []
            for e in reader2:
                registros.append(e)
            return len(registros)
        

def truncateTable(table):
    path = './tables/'+table+'.avro'
    if isEnabled(table):
        cfs = []
        with open(path, 'rb') as f:
            reader = fastavro.reader(f)
            reader2 = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            schema = reader.writer_schema
            schema_dict = schema
            
            for e in schema_dict['fields']:
                if e['name'] != 'rowkey':
                    #e['type']['fields'] = []
                    cfs.append(e['name'])
        print(disable(table))
        print(dropTable(table))
        print(create_table(table, cfs))
        print('Table created successfully')
    else:
        print("Table is disabled")


def scanTable(table):
    path = './tables/'+table+'.avro'
    if isEnabled(table):
        with open(path, 'rb') as f:
            reader = fastavro.reader(f)
            reader2 = avro.datafile.DataFileReader(f, avro.io.DatumReader())
            schema = reader.writer_schema
            schema_dict = schema
            registros = []
            for e in reader2:
                registros.append(e)
            return registros
    else:
        return "Table is disabled"


# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

def is_double(s):
    try:
        float(s)
        return True
    except ValueError:
        return False

def is_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False
    
import ast

def is_bool(s):
    if s == 'True' or s == 'False':
        return True
    else:
        return False


def TypeOf(value):
    # value is a string
    if is_int(value):
        return 'int', int(value)
    elif is_double(value):
        return 'double', float(value)
    elif is_bool(value):
        return 'boolean', ast.literal_eval(value)
    else:
        return 'string', value
    
def createDefault(data_type):
    if data_type == 'int':
        return 0
    elif data_type == 'double':
        return 0.0
    elif data_type == 'boolean':
        return False
    elif data_type == 'string':
        return '-'