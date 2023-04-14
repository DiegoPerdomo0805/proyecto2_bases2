import fastavro
import glob




def check_table(table):
    if table == '':
        return False
    else:
        found = False
        tables = glob.glob('./tables/*.avro')
        for t in tables:
            with open(t, 'rb') as f:
                f_schema = fastavro.schema.load_schema(f)
                if f_schema['name'] == table:
                    found = True
                    break
        return found
                