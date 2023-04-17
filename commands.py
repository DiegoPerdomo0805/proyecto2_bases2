from connection import * 


class command:
    def __init__(self, cmd):
        self.cmd = cmd[0]
        #print(type(self.cmd))
        self.args = cmd[1:]
        #print(type(self.args))
        if self.cmd == 'create':
            self.cmdCreate()
        elif self.cmd == 'list':
            self.cmdList()
        elif self.cmd == 'disable':
            self.cmdDisable()
        elif self.cmd == 'enable':
            self.cmdEnable()
        elif self.cmd == 'is_enabled':
            self.cmdIsEnabled()
        elif self.cmd == 'alter':
            self.cmdAlter()
        elif self.cmd == 'drop':
            self.cmdDrop()
        elif self.cmd == 'drop_all':
            self.cmdDropAll()
        elif self.cmd == 'describe':
            self.cmdDescribe()
        elif self.cmd == 'put':
            self.cmdPut()
        elif self.cmd == 'get':
            self.cmdGet()
        elif self.cmd == 'scan':
            self.cmdScan()
        elif self.cmd == 'delete':
            self.cmdDelete()
        elif self.cmd == 'delete_all':
            self.cmdDeleteAll()
        elif self.cmd == 'count':
            self.cmdCount()
        elif self.cmd == 'truncate':
            self.cmdTruncate()


# funciones ddl: create, list, disable, enable, is_enabled, alter, drop, drop all, describe
        
    def cmdCreate(self):
        limit = 10
        if len(self.args) < 2:
            print('Error: faltan argumentos')
        elif len(self.args) > limit:
            print('Error: no se pueden declarar mas de ' + str(limit-1) + ' familias de columnas')
        else:
            if check_table(self.args[0]):
                print('Error: la tabla ya existe')
            else:
                name = self.args[0]
                c_families = self.args[1:]
                # crear la tabla
                for i in range(len(c_families)):
                    if c_families[i] == '__void__':
                        print('Error: no se puede usar la familia de columnas __void__')
                        return
                # crear el archivo
                create_table(name, c_families)
                print('Tabla creada con éxito: ', name, c_families)


    def cmdList(self):
        if len(self.args) > 0:
            print('Error: no se esperan argumentos')
        else:
            tables = listTable()
            for e in tables:
                print(e)

    def cmdDisable(self):
        if len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        elif len(self.args) < 1:
            print('Error: faltan argumentos')
        else:
            if check_table(self.args[0]):
                # deshabilitar la tabla
                if not isEnabled(self.args[0]):
                    print('Error: la tabla ya esta deshabilitada')
                else:
                    print(disable(self.args[0]))
            else:
                print('Error: la tabla no existe')

    def cmdEnable(self):
        if len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        elif len(self.args) < 1:
            print('Error: faltan argumentos')
        else:
            if check_table(self.args[0]):
                # habilitar la tabla
                if isEnabled(self.args[0]):
                    print('Error: la tabla ya esta habilitada')
                else:
                    print(enable(self.args[0]))
            else:
                print('Error: la tabla no existe')

    def cmdIsEnabled(self):
        if len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        elif len(self.args) < 1:
            print('Error: faltan argumentos')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                if isEnabled(self.args[0]):
                    print('La tabla ' + self.args[0] + ' esta habilitada')
                else:
                    print('La tabla ' + self.args[0] + ' no esta habilitada')
            else:
                print('Error: la tabla no existe')

    def cmdAlter(self):
        if len (self.args) < 3:
            print('Error: faltan argumentos')
        elif len(self.args) > 4:
            print('Error: Sólo se requieren 3 o 4 argumentos')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                # verificar si la familia de columnas existe
                # verificar si la familia de columnas esta habilitada
                if self.args[1] == 'rename' or  self.args[1] == 'drop':
                    #print('Familia de columnas modificada')
                    action = self.args[1]
                    if action == 'rename' and len(self.args) == 4:
                        # renombrar la familia de columnas
                        # modificar el schema
                        # modificar el archivo
                        print('Familia de columnas renombrada')
                    elif action == 'drop' and len(self.args) == 3:
                        # eliminar la familia de columnas
                        # modificar el schema
                        # modificar el archivo
                        print('Familia de columnas eliminada')
                    else:
                        print('Error: el comando no existe o argumentos incorrectos')
                else:
                    print('Error: el comando no existe')
            else:
                print('Error: la tabla no existe')

    def cmdDrop(self):
        if len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        elif len(self.args) < 1:
            print('Error: faltan argumentos')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                # eliminar la tabla
                # eliminar el archivo
                # eliminar el schema
                # eliminar el nombre de la tabla de la lista de tablas
                # eliminar el nombre de la tabla del archivo de tablas
                # eliminar el nombre de la tabla del archivo de schemas
                print(dropTable(self.args[0]))
            else:
                print('Error: la tabla no existe')

    def cmdDropAll(self):
        if len(self.args) > 0:
            print('Error: no se esperan argumentos')
        else:
            # eliminar todas las tablas
            # eliminar el archivo de tablas
            # eliminar el archivo de schemas
            print(dropAllTables())
    
    def cmdDescribe(self):
        if len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        elif len(self.args) < 1:
            print('Error: faltan argumentos')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                # mostrar el schema
                print('Tabla')
            else:
                print('Error: la tabla no existe')



# ////////////////////////////////////////////////              DML              //////////////////////////////////////////////////////////////////////////////////
# funciones dml: put, get, scan, delete, delete all, count, truncate


    def cmdPut(self):
        if len(self.args) < 5:
            print('Error: faltan argumentos')
        else:
            if check_table(self.args[0]):
                table = self.args[0]
                rowkey = self.args[1]
                c_family = self.args[2]
                column = self.args[3]
                value = self.args[4:]
                value = ' '.join(value)
                #print(value, type(value))
                if is_int(rowkey):
                    rowkey = int(rowkey)
                    print(putTable(table, rowkey, c_family, column, value))
            else:
                print('Error: la tabla no existe')


    def cmdGet(self):
        if len(self.args) < 2:
            print('Error: faltan argumentos')
        elif len(self.args) > 2:
            print('Error: Sólo se requieren 2 argumentos')
        else:
            if check_table(self.args[0]):
                table = self.args[0]
                rowkey = self.args[1]
                if is_int(rowkey):
                    rowkey = int(rowkey)
                    result = getTable(table, rowkey)
                    if result != "Rowkey does not exist":
                        for e in result:
                            print(e , ': ' , result[e])
                    else:
                        print(result)
                else:
                    print('Error: el rowkey debe ser un número entero')
            else:
                print('Error: la tabla no existe')

    def cmdScan(self):
        if len(self.args) < 1:
            print('Error: faltan argumentos')
        elif len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                print('Valores obtenidos')
            else:
                print('Error: la tabla no existe')

    def cmdDelete(self):
        if len(self.args) < 4:
            print('Error: faltan argumentos')
        elif len(self.args) > 4:
            print('Error: Sólo se requieren 4 argumentos')
        else:
            if check_table(self.args[0]):
                table = self.args[0]
                rowkey = self.args[1]
                c_family = self.args[2]
                column = self.args[3]
                if is_int(rowkey):
                    rowkey = int(rowkey)
                    print(deleteTable(table, rowkey, c_family, column))
                else:
                    print('Error: el rowkey debe ser un número entero')
            else:
                print('Error: la tabla no existe')

    def cmdDeleteAll(self):
        if len(self.args) < 2:
            print('Error: faltan argumentos')
        elif len(self.args) > 2:
            print('Error: Sólo se requieren 2 argumentos')    
        else:
            if check_table(self.args[0]):
                table = self.args[0]
                rowkey = self.args[1]
                if is_int(rowkey):
                    rowkey = int(rowkey)
                    print(deleteAllTable(table, rowkey))
                else:
                    print('Error: el rowkey debe ser un número entero')
            else:
                print('Error: la tabla no existe')    

    def cmdCount(self):
        if len(self.args) < 1:
            print('Error: faltan argumentos')
        elif len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        else:
            if check_table(self.args[0]):
                print('Número de filas en la tabla ', self.args[0], ' es: ', countTable(self.args[0]))
            else:
                print('Error: la tabla no existe')

    def cmdTruncate(self):
        if len(self.args) < 1:
            print('Error: faltan argumentos')
        elif len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        else:
            if check_table(self.args[0]):
                truncateTable(self.args[0])
            else:
                print('Error: la tabla no existe')
