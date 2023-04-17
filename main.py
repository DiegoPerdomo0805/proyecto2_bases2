from connection import *
from commands import command


# funciones ddl: create, list, disable, enable, is_enabled, alter, drop, drop all, describe

# funciones dml: put, get, scan, delete, delete all, count, truncate

cmds = [
    'create',
    'list',
    'disable',
    'enable',
    'is_enabled',
    'alter',
    'drop',
    'drop_all',
    'describe',
    'put',
    'get',
    'scan',
    'delete',
    'delete_all',
    'count',
    'truncate'
]

running = True


while running:
    cmd = input('\nfastavro> ')
    cmd = cmd.lower()
    cmd = cmd.strip()
    # convertir a lista separando por espacios
    cmd = cmd.split(' ')
    if cmd[0] == 'exit':
        running = False
    elif cmd[0] in cmds:
        r = command(cmd)
    else:
        print('comando no reconocido')



