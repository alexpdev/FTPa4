import os

def syncremote(client,remote,local):
    contents = os.listdir(local)
    for path in client.nlst(remote):
        name = os.path.split(path)[-1]
        if name not in contents:
            get_path(client,path,local)
        else:
            lpath = os.path.join(local,name)
            if os.path.isdir(lpath):
                find_incomplete(client,path,lpath)
    return "Completed"

def find_incomplete(client,remote,local):
    lst = os.listdir(local)
    for item in client.nlst(remote):
        name = os.path.basename(item)
        if name in lst:
            path = os.path.join(local,name)
            if os.path.isdir(path):
                find_incomplete(client,item,path)
        else:
            get_path(client, item, local)


def get_path(client, path, parent):
    name = os.path.split(path)[-1]
    local = os.path.join(parent,name)
    with open(local,"wb") as fp:
        client.retrbinary(f'RETR {path}', fp.write)
    client.dir(path)
    lst = client.nlst(path)
    child = os.path.join(parent,name)
    os.mkdir(child)
    for p in lst:
        get_path(client,p, child)
