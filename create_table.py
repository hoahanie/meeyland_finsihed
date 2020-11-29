import psycopg2
from sshtunnel import SSHTunnelForwarder
import sys

# Create an SSH tunnel
tunnel = SSHTunnelForwarder(
    ('45.119.81.84', 22),
    ssh_username='root',
    #ssh_private_key='</path/to/private/key>',
    ssh_password='qW7iO2Tgt1Tw',
    remote_bind_address=('localhost', 5432),
    local_bind_address=('localhost', 6543), # could be any available port
)
# Start the tunnel
tunnel.start()
try:
    conn = psycopg2.connect(
    database='postgres',
    user='vtttuong', 
    password='Tuongro26**',  
    host=tunnel.local_bind_host,
    port=tunnel.local_bind_port,
    )
    cur = conn.cursor()
    cur.execute("""CREATE TABLE meeyland_muaban(
                    _id TEXT PRIMARY KEY NOT NULL,
                    updatedDate TEXT,
                    metaDescription TEXT,
                    area TEXT,
                    creatorType TEXT,
                    sortGeoloc TEXT,
                    geoloc TEXT,
                    unitPrice TEXT,
                    totalPrice TEXT,
                    media TEXT,
                    title TEXT,
                    startTime TEXT,
                    creator_phone TEXT,
                    creator_name TEXT,
                    creator_id TEXT,
                    creator_email TEXT,
                    creator_type TEXT,
                    createdDate TEXT,
                    location TEXT,
                    endTime TEXT,
                    category_id TEXT,
                    slug TEXT,
                    "5dfb2acdd5e511385e90df86" TEXT,
                    "5dfb2af5d5e511385e90df91" TEXT,
                    "5dfb2b34d5e511385e90df9c" TEXT,
                    "5e4f47f1d86a7b3d53d59ae7" TEXT,
                    "5dfa5e0359a281c7221c2335" TEXT,
                    "5df6630bba09ec22616c3532" TEXT,
                    "5dfb7072d5e511385e90e01c" TEXT,
                    "5df65f90eb02bf5dff0fbffe" TEXT,
                    "5df66112eb02bf5dff0fc009" TEXT,
                    "5dfa597659a281c7221c2324" TEXT,
                    "5dfa5f3e59a281c7221c2340" TEXT,
                    "5dfa723059a281c7221c23b3" TEXT,
                    "5dfa741659a281c7221c23c4" TEXT,
                    "5dfa74d559a281c7221c23d4" TEXT,
                    "5dfa788e59a281c7221c2405" TEXT,
                    "5dfa790459a281c7221c2410" TEXT,
                    "5e4f8282eac2cf6ac3432b38" TEXT,
                    "5e5020a2d4f8a9c5471e12d2" TEXT,
                    "5e502100d4f8a918891e12d3" TEXT,
                    "5e502133d4f8a977781e12d4" TEXT,
                    "5e50dbbc7fb8300f47140100" TEXT,
                    "5e502177d4f8a9528b1e12d6" TEXT,
                    "5e50dc037fb8301180140102" TEXT,
                    "5e50dd20f0543b8a805b27f5" TEXT,
                    "5e50dcb9f0543bfcdb5b27f4" TEXT );
                """)

    conn.commit()
    # cur.execute("select * from information_schema.tables")
    # db_version = cur.fetchone()
    # print(db_version)
    # # execute a statement
    # print('PostgreSQL database version:')
    # cur.execute('SELECT version()')

    # # display the PostgreSQL database server version
    # db_version = cur.fetchone()
    # print(db_version)
       
	# close the communication with the PostgreSQL
    cur.close()
except (psycopg2.OperationalError, Exception, psycopg2.DatabaseError) as e:
    print('Unable to connect!\n{0}'.format(e))
    sys.exit(1)
else:
    print('Connected!')
finally:
    if conn is not None:
        print('Closed !!!')
        conn.close()
    tunnel.stop()