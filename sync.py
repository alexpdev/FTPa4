import logging
import os
from sftpc.ftpdirsync import Client
from sftpc.utils import traverse, Pool
import dotenv
dotenv.load_dotenv()

logger = logging.getLogger(__name__)

un = os.environ['UN']
pw = os.environ['PW']
pt = os.environ['PT']
hn = os.environ['HN']
LOCAL = os.environ['LOCAL']
REMOTE = os.environ['REMOTE']


def main():
    pool = Pool(3)
    client = Client()
    client.connect(host=hn, port=pt)
    client.login(user=un, passwd=pw)
    local = LOCAL
    remote = REMOTE
    traverse(remote, local, client, pool)

if __name__ == "__main__":
    main()
