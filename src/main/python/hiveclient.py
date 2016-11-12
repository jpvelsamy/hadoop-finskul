import pyhs2 as hive
DEFAULT_DB = 'default'
DEFAULT_SERVER = '172.17.0.2'
DEFAULT_PORT = 10000
u="jpvel"
p="jpvel"
connection = hive.connect(host=DEFAULT_SERVER, port= DEFAULT_PORT, authMechanism="PLAIN", user=u, password=p)
statement = "SELECT COUNT(*) FROM FINSKUL.MFG_MASTER"
cur=connection.cursor()
cur.execute(statement)
df = cur.fetchall()
print df
