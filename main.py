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
    'truncate',
]

running = True


def help():

    print('\nComandos disponibles y sus sintáxis:')
    with open('./extra/commands_syntax.txt', 'r') as f:
        for e in f:
            print(e, end='')
    print('\n')


while running:
    cmd = input('\nNEKR0N> ')
    cmd = cmd.lower()
    cmd = cmd.strip()
    # convertir a lista separando por espacios
    cmd = cmd.split(' ')
    if cmd[0] == 'exit':
        running = False
    elif cmd[0] == 'help':
        help()
    elif cmd[0] in cmds:
        r = command(cmd)
    else:
        print('comando no reconocido')



