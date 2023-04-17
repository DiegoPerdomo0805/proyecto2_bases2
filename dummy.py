import fastavro
from commands import command
import avro

print(fastavro.__version__)

#print(schema)



"""path = './tables/alfa.avro'

c_families = ['beta','gamma']

from connection import create_table, dropTable

dropTable('alfa')

create_table('alfa', c_families)

"""
#dropTable('alfa')

#create_table('alfa', c_families)


print('\n\n\n\n - Alfa')

with open('./tables/alfa.avro', 'rb') as f:
    reader = fastavro.reader(f)
    schema = reader.writer_schema
    print(schema)
    print(type(schema))

print('\ncampos')


for e in schema['fields']:
    if e['name'] != 'rowkey':
        print(e['name'])


"""
cf = 'beta'

# check if column family exists
cf_exists = False

for e in schema['fields']:
    if e['name'] == cf:
        cf_exists = True
        break

print(cf_exists)

#print(schema['fields'])
for e in schema['fields']:
    #print(e['name'])
    if e['name'] == cf:
        for e2 in e['type']['fields']:
            if e2['name'] == '__void__':
                i = e['type']['fields'].index(e2)
                print(e['type']['fields'])
                print(e['type']['fields'][i])
                print(i)
                break


record = {
    "rowkey": 1
}

print(schema['fields'])

for e in schema['fields']:
    if e['name'] != 'rowkey':
        print(e['name'])
        record[e['name']] = {"__void__":"__void__"}

print(record)"""


# write new record

#with open('./tables/alfa.avro', 'wb') as f:
#    fastavro.writer(f, schema, [record])

print('\n\n\n\n--------------------------------------------------\n\n\n\n')



with open("./tables/alfa.avro", "rb") as avro_file:
    reader = avro.datafile.DataFileReader(avro_file, avro.io.DatumReader())
    print(reader)
    for record in reader:
        print(' - ',record,'\n')

    reader.close()



from connection import putTable

#print(putTable('alfa', 1, 'beta', 'name', 'John Smith'))
"""
record2 = {
    "rowkey": 2,
}

for e in schema['fields']:
    if e['name'] != 'rowkey':
        record2[e['name']] = {"__void__":"__void__"}

print(record2)

with open("./tables/alfa.avro", "wb") as avro_file:
    fastavro.writer(avro_file, schema, [record, record2])"""

"""
registros = []
with open("./tables/alfa.avro", "rb") as avro_file:
    reader = avro.datafile.DataFileReader(avro_file, avro.io.DatumReader())
    for record in reader:
        print(' - ',record, type(record))

    reader.close()


from connection import disable, enable, isEnabled

print(' La tabla está habilitada? ',isEnabled('alfa'))

print('\n')
from connection import putTable
print(putTable('alfa', 5, 'beta', 'name', 'John Smith'))



print('\n')
for e in registros:
    print(e)

print('\n\n\n\n - Alfa')

with open('./tables/alfa.avro', 'rb') as f:
    reader = fastavro.reader(f)
    schema = reader.writer_schema
    print(schema)
    print(type(schema))

print('\n')


registros = []
with open("./tables/alfa.avro", "rb") as avro_file:
    reader = avro.datafile.DataFileReader(avro_file, avro.io.DatumReader())
    for record in reader:
        print(' - ',record, type(record))
        #registros.append(record)

    reader.close()"""

"""
trial_values = ['3.1416', '5', 'Jason Momoa', 'True']

from connection import TypeOf

for e in trial_values:
    t, v = TypeOf(e)
    print(t, v, type(v))"""



"""from connection import create_table, dropTable

families = ['grog', 'pike', 'percy']
create_table('vox_machina', families)

with open('./tables/vox_machina.avro', 'rb') as f:
    reader = fastavro.reader(f)
    schema = reader.writer_schema
    print(schema)
    print(type(schema))

dropTable('vox_machina')"""


#with open('./tables/example.avsc', 'rb') as f:
#    schema_bytes = io.BytesIO(f.read())

#schema = fastavro.parse_schema(schema_bytes)

#schema = fastavro.parse_schema(binary)


"""import avro.datafile
import avro.io

with open('./tables/example.avro', 'rb') as avro_file:
    reader = avro.datafile.DataFileReader(avro_file, avro.io.DatumReader())
    schema = reader.meta
    reader.close()

print(schema,'\n\n')
print(schema['avro.schema'],'\n\n')


import json

schema_str = schema['avro.schema'].decode('utf-8')
schema_dict = json.loads(schema_str)
print(schema_dict)
print(schema_dict['name'])
print(type(schema_dict))

if 'cf3' in schema_dict['fields']:
    print('existe')
else:
    print('no existe')"""

## 
## # Load the schema
## schema = avro.schema.parse(open("./tables/example.avsc").read())
## 
## """# Read data from the file
## with open("./tables/example.avro", "rb") as avro_file:
##     reader = avro.datafile.DataFileReader(avro_file, avro.io.DatumReader())
##     print(reader)
##     for record in reader:
##         print(' - ',record,'\n')
##         print(' - ',record['cf1'],'\n')
##         print(' - ',record['cf1']['name'],'\n')
##     reader.close()