# Proyecto de emulación de HBase con formato Avro para economía de almacenamiento y eficiencia de solicitudes.

## funciones ddl: create, list, disable, enable, is_enabled, alter, drop, drop all, describe

    create my_table columnfamilyname 

    list 

    disable my_table 

    enable my_table 

    is_enabled my_table 

    alter my_table rename columnfamilyname newcolumnfamilyname 

    alter my_table drop columnfamilyname 

    drop my_table 

    drop all 

    describe my_table

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

## funciones dml: put, get, scan, delete, delete all, count, truncate

    put my_table rowkey columnfamilyname columnname value 
    
  *put can add or update a row*

    get my_table rowkey 

    scan my_table 

    delete my_table rowkey columnfamilyname columnname 

    deleteall my_table rowkey 

    count my_table

    truncate my_table

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
