from connection import * 


class command:
    def __init__(self, cmd):
        self.cmd = cmd[0]
        print(type(self.cmd))
        self.args = cmd[1:]
        print(type(self.args))
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
                # crear la tabla
                # crear el archivo
                # crear el schema
                # guardar el schema en el archivo
                # agregar el archivo a la lista de archivos
                # agregar el schema a la lista de schemas
                # agregar el nombre de la tabla a la lista de tablas
                # agregar el nombre de la tabla al archivo de tablas
                # agregar el nombre de la tabla al archivo de schemas
                # agregar el n
                print('Tabla creada')


    def cmdList(self):
        if len(self.args) > 0:
            print('Error: no se esperan argumentos')

    def cmdDisable(self):
        if len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        elif len(self.args) < 1:
            print('Error: faltan argumentos')
        else:
            if check_table(self.args[0]):
                # deshabilitar la tabla
                print('Tabla deshabilitada')
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
                print('Tabla habilitada')
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
                print('Tabla habilitada')
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
                    print('Familia de columnas modificada')
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
                print('Tabla eliminada')
            else:
                print('Error: la tabla no existe')

    def cmdDropAll(self):
        if len(self.args) > 0:
            print('Error: no se esperan argumentos')
    
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


# funciones dml: put, get, scan, delete, delete all, count, truncate


    def cmdPut(self):
        if len(self.args) < 5:
            print('Error: faltan argumentos')
        elif len(self.args) > 5:
            print('Error: Sólo se requieren 5 argumentos')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                # verificar si la familia de columnas existe
                # verificar si la familia de columnas esta habilitada
                # verificar si la columna existe
                # verificar si la columna esta habilitada
                # verificar si el valor es del tipo correcto
                # verificar si el valor es del tamaño correcto
                # verificar si el valor es del formato correcto
                # verificar si el valor es del rango correcto
                # verificar si el valor es del dominio correcto
                print('Valor insertado')
            else:
                print('Error: la tabla no existe')


    def cmdGet(self):
        if len(self.args) < 2:
            print('Error: faltan argumentos')
        elif len(self.args) > 2:
            print('Error: Sólo se requieren 2 argumentos')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                # verificar si la familia de columnas existe
                # verificar si la familia de columnas esta habilitada
                # verificar si la columna existe
                # verificar si la columna esta habilitada
                # verificar si el valor es del tipo correcto
                # verificar si el valor es del tamaño correcto
                # verificar si el valor es del formato correcto
                # verificar si el valor es del rango correcto
                # verificar si el valor es del dominio correcto
                print('Valor obtenido')
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
                # verificar si la tabla esta habilitada
                # verificar si la familia de columnas existe
                # verificar si la familia de columnas esta habilitada
                # verificar si la columna existe
                # verificar si la columna esta habilitada
                # verificar si el valor es del tipo correcto
                # verificar si el valor es del tamaño correcto
                # verificar si el valor es del formato correcto
                # verificar si el valor es del rango correcto
                # verificar si el valor es del dominio correcto
                print('Valor eliminado')
            else:
                print('Error: la tabla no existe')

    def cmdDeleteAll(self):
        if len(self.args) < 2:
            print('Error: faltan argumentos')
        elif len(self.args) > 2:
            print('Error: Sólo se requieren 2 argumentos')    
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                # verificar si la familia de columnas existe
                # verificar si la familia de columnas esta habilitada
                print('Valores eliminados')
            else:
                print('Error: la tabla no existe')    

    def cmdCount(self):
        if len(self.args) < 1:
            print('Error: faltan argumentos')
        elif len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                print('Valores contados')
            else:
                print('Error: la tabla no existe')

    def cmdTruncate(self):
        if len(self.args) < 1:
            print('Error: faltan argumentos')
        elif len(self.args) > 1:
            print('Error: Sólo se requiere el nombre de la tabla')
        else:
            if check_table(self.args[0]):
                # verificar si la tabla esta habilitada
                print('Tabla truncada')
            else:
                print('Error: la tabla no existe')
