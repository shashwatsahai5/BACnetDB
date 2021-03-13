import psycopg2
import datetime

#default credentials for database connection, modify according to your database
database = "BACnetDB"
user = "postgres"
password = "Mumbai@2021"
host = "127.0.0.1"
port = "5432"


class JCI_DBGateway:
    def __init__(self):
        try:
            self.db_con = psycopg2.connect(database=database, user = user, password = password, host = host, port = port)       
            #print ("DB Connected!")
        except:
            print("DB Error")

        self.populate_database(self)

    def populate_database(self, args):
        try:
            cur = self.db_con.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS gateway (
                id VARCHAR(64) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                attributes VARCHAR(255),
                sent_to_cloud BOOLEAN NOT NULL,
                sent_to_cloud_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
                );
            ''')

            #print("Gateway Table Created!")

            self.db_con.commit()
            #self.db_con.close()
        except Exception as e:
            print("Couldn't create Gateway Table :(\n")
            print(e)

    def insert(self, data):
        
        sql = """INSERT INTO gateway(
            id, 
            name, 
            attributes, 
            sent_to_cloud, 
            sent_to_cloud_timestamp)
            VALUES (
                '%(id)s', 
                '%(name)s', 
                '%(attributes)s', 
                '%(sent_to_cloud)s', 
                '%(sent_to_cloud_timestamp)s')"""

        cur = self.db_con.cursor()
        cur.execute(sql %{
            'id' : data['id'],
            'name' : data['name'],
            'attributes' : data['attributes'],
            'sent_to_cloud' : '0',
            'sent_to_cloud_timestamp' : datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")
        })
        self.db_con.commit()
        print("Row Added!")

    def sendToCloud(self, id):
        sql = """UPDATE gateway SET sent_to_cloud = 'true', sent_to_cloud_timestamp = '%(current_timestamp)s' WHERE id = '%(ID)s'"""
        cur = self.db_con.cursor()
        cur.execute(sql %{'current_timestamp':datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 'ID':id})
        self.db_con.commit()
        print("Sent To Cloud Sucessfully!")

class JCI_DBDevice:
    def __init__(self):
        try:
            self.db_con = psycopg2.connect(database=database, user = user, password = password, host = host, port = port)       
            #print ("DB Connected!")
        except:
            print("DB Error")

        self.populate_database(self)

    def populate_database(self, args):
        try:
            cur = self.db_con.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS device (
                id VARCHAR(64) PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                instance VARCHAR(255),
                ip_address VARCHAR(255),
                status VARCHAR(255),
                properties VARCHAR(255),
                created_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                sent_to_cloud BOOLEAN NOT NULL,
                sent_to_cloud_timestamp TIMESTAMP WITH TIME ZONE NOT NULL
                );
            ''')

            #print("Device Table Created!")

            self.db_con.commit()
            #self.db_con.close()
        except Exception as e:
            print("Couldn't create Device Table :(\n")
            print(e)

    def insert(self, data):
        
        sql = """INSERT INTO device(
            id, 
            name, 
            instance, 
            ip_address, 
            status, 
            properties, 
            created_timestamp, 
            sent_to_cloud, 
            sent_to_cloud_timestamp)
            
            VALUES ('{id}', 
            '{name}', 
            '{instance}', 
            '{ip_address}', 
            '{status}', 
            '{properties}', 
            '{created_timestamp}', 
            'false', 
            '{sent_to_cloud_timestamp}');"""

        cur = self.db_con.cursor()
        cur.execute(sql.format(
            id = data['id'],
            name = data['name'],
            instance = data['instance'],
            ip_address = data['ip_address'],
            status = data['status'],
            properties = data['properties'],
            created_timestamp = datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"),
            sent_to_cloud_timestamp = datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"),
        ))
        self.db_con.commit()
        print("Row Added!")

    def sendToCloud(self, id):
        sql = """UPDATE device SET sent_to_cloud = 'true', sent_to_cloud_timestamp = '%(current_timestamp)s' WHERE id = '%(ID)s'"""
        cur = self.db_con.cursor()
        cur.execute(sql %{'current_timestamp':datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 'ID':id})
        self.db_con.commit()
        print("Sent To Cloud Sucessfully!")

    def changeStatus(self, id, status):
        sql = """
            UPDATE device SET status = '%(Status)s' WHERE id = '%(ID)s'
        """
        cur = self.db_con.cursor()
        cur.execute(sql %{'Status':status, 'ID':id})
        self.db_con.commit()
        print("Status changed Sucessfully!")

class JCI_DBPoints:
    def __init__(self):
        try:
            self.db_con = psycopg2.connect(database=database, user = user, password = password, host = host, port = port)       
            #print ("DB Connected!")
        except:
            print("DB Error")

        self.populate_database(self)

    def populate_database(self, args):
        try:
            cur = self.db_con.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS points (
                id VARCHAR(64) PRIMARY KEY,
                device_id VARCHAR(64),
                name VARCHAR(255) NOT NULL,
                device_ip VARCHAR(255),
                type VARCHAR(255),
                instance VARCHAR(255),
                properties VARCHAR(255),
                polled VARCHAR(255),
                subscribed BOOLEAN,
                created_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                sent_to_cloud BOOLEAN NOT NULL,
                sent_to_cloud_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                CONSTRAINT fk_device
                    FOREIGN KEY(device_id)
                        REFERENCES device(id)
                        ON DELETE SET NULL
                );
            ''')

            #print("Points Table Created!")

            self.db_con.commit()
            #self.db_con.close()
        except Exception as e:
            print("Couldn't create Points Table :(\n")
            print(e)

    def insert(self, data):
        sql = """
            INSERT INTO points(
                id, 
                device_id, 
                name, 
                device_ip, 
                type, 
                instance, 
                properties, 
                polled, 
                subscribed, 
                created_timestamp, 
                sent_to_cloud, 
                sent_to_cloud_timestamp)
            VALUES ('%(id)s', 
            '%(device_id)s', 
            '%(name)s', 
            '%(device_ip)s', 
            '%(type)s', 
            '%(instance)s', 
            '%(properties)s', 
            '%(polled)s', 
            '%(subscribed)s', 
            '%(created_timestamp)s', 
            '%(sent_to_cloud)s', 
            '%(sent_to_cloud_timestamp)s');
        """

        cur = self.db_con.cursor()
        cur.execute(sql %{
            'id':data['id'],
            'device_id':data['device_id'],
            'name':data['name'],
            'device_ip':data['device_ip'],
            'type': data['type'],
            'instance': data['instance'],
            'properties':data['properties'],
            'polled':data['polled'],
            'subscribed': '0',
            'created_timestamp': datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"),
            'sent_to_cloud':'0',
            'sent_to_cloud_timestamp':datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")

        })
        #print("Row Added!")
        self.db_con.commit()

    def sendToCloud(self, id):
        sql = """UPDATE points SET sent_to_cloud = 'true', sent_to_cloud_timestamp = '%(current_timestamp)s' WHERE id = '%(ID)s'"""
        cur = self.db_con.cursor()
        cur.execute(sql %{'current_timestamp':datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"), 'ID':id})
        self.db_con.commit()
        print("Sent To Cloud Sucessfully!")

class JCI_DBTelemetry:
    def __init__(self):
        try:
            self.db_con = psycopg2.connect(database=database, user = user, password = password, host = host, port = port)       
            #print ("DB Connected!")
        except:
            print("DB Error")

        self.populate_database(self)

    def populate_database(self, args):
        try:
            cur = self.db_con.cursor()
            cur.execute('''
                CREATE TABLE IF NOT EXISTS telemetery (
                id VARCHAR(64) PRIMARY KEY,
                point_id VARCHAR(64) NOT NULL,
                payload VARCHAR(255),
                timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
                CONSTRAINT fk_point
                    FOREIGN KEY(point_id)
                        REFERENCES points(id)
                        ON DELETE SET NULL
                );
            ''')

            #print("Telemetery Table Created!")

            self.db_con.commit()
            #self.db_con.close()
        except Exception as e:
            print("Couldn't create Telemetery Table :(\n")
            print(e)

    def insert(self, data):
        sql = """INSERT INTO public.telemetery(
	        id, 
            point_id, 
            payload, 
            timestamp)
            VALUES (
                '%(id)s', 
                '%(point_id)s', 
                '%(payload)s', 
                '%(timestamp)s')"""

        cur = self.db_con.cursor()
        cur.execute(sql %{
            'id' : data['id'],
            'point_id' : data['point_id'],
            'payload' : data['payload'],
            'timestamp' : datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")
        })
        self.db_con.commit()
        print("Row Added!")




#Insert into device table example
"""
device = JCI_DBDevice()

device_data = {
    'id':'4',
    'name':'MC102',
    'instance':'1022154',
    'ip_address':'192.168.0.10',
    'status':'active',
    'properties': 'xyz'

}

device.insert(device_data)
"""
#Insert into gateway table example
"""
gateway = JCI_DBGateway()

gateway_data = {
    'id':'2',
    'name':'GW001',
    'attributes':'xyz',
    
}

gateway.insert(gateway_data)
"""

#Insert into points table example
"""
point = JCI_DBPoints()

point_data = {
    'id':'2',
    'device_id':'1',
    'name':'MN1012',
    'device_ip':'192.168.0.10',
    'type': 'A',
    'instance': '1254468',
    'properties':'abc def ghi jkl',
    'polled':'xyz',
    'subscribed': '0',
    'created_timestamp': datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)"),
    'sent_to_cloud':'0',
    'sent_to_cloud_timestamp':datetime.datetime.now().strftime("%d-%b-%Y (%H:%M:%S.%f)")

}

point.insert(point_data)
"""

#Insert into telemetery table example
"""
tele = {
    'device_id': 5,
    'point_id':2
}
telemetery = JCI_DBTelemetry()
telemetery_data = {
    'id': tele['device_id'],
    'point_id':'1',
    'payload':'xyz'
    
}

telemetery.insert(telemetery_data)
"""

#device = JCI_DBDevice()
#device.sendToCloud(1)
#point = JCI_DBPoints()
#point.sendToCloud(1)

#device = JCI_DBDevice()
#status = "Locked"
#device.changeStatus(2,status)

#gateway = JCI_DBGateway()
#gateway.sendToCloud(1)