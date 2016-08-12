import urllib.request         #Module for reading from the web
import csv
import pypyodbc as pyodbc
import sys
from datetime import datetime
from bs4 import BeautifulSoup

def wrap_and_encode(x):
    return ("'%s'" % x)

user='sa'
password='-'
database='statebike'
port='1433'
TDS_Version='8.0'
server='localhost'
driver='{SQL Server Native Client 11.0}'
#Driver={SQL Server Native Client 11.0};Server=myServerAddress;Database=myDataBase;Uid=myUsername;Pwd=myPassword;

con_string='Uid=%s;Pwd=%s;Database=%s;Server=%s;driver=%s' % (user,password,database,server,driver)
cnxn=pyodbc.connect(con_string)
cursor=cnxn.cursor()

try:    
    response = urllib.request.urlopen("https://recursos-data.buenosaires.gob.ar/ckan2/ecobici/estado-ecobici.xml")
    html = response.read()        #Gets an array of bytes!
    strXML = html.decode()       #Convert to a string
except:
    sqltext =("INSERT INTO estado ([estacionid],[bicicletasdisponibles],[numero],[anclajedisponible]) VALUES (%s,%s,%s,%s)" %('0','0','0','0'))
    cursor.execute(sqltext)
    cnxn.commit()
    print ('Error HTTP')
    sys.exit(0)

filename1 = 'bike-stations-'+datetime.now().strftime("%Y%m%d-%H%M%S")+'.xml'
filename2 = 'bike-stations-'+datetime.now().strftime("%Y%m%d-%H%M%S")+'.csv'
print (filename1)

#handler = open('bike-stations-20160615-163605.xml').read()
soup=BeautifulSoup(strXML)

print (soup.find('estacion').find('estacionid').text)

#myfile = open(filename2, "w")
#writer = csv.writer(myfile)
trenn=','

#print (con_string)
cnxn=pyodbc.connect(con_string)
cursor=cnxn.cursor()

for message in soup.findAll('estacion'):
        f_estid = message.find('estacionid').text
        f_estnom = message.find('estacionnombre').text
        f_bicdisp = message.find('bicicletadisponibles').text
        f_estdisp = message.find('estaciondisponible').text
        f_num = message.find('numero').text
        f_lugar = message.find('lugar').text
        f_anclajetot = message.find('anclajestotales').text
        f_ancdisp = message.find('anclajesdisponibles').text
        #sqltext =("INSERT INTO estado ([estacionid],[estaciondisponible],[bicicletasdisponibles],[numero],[lugar],[anclajetotal],[anclajedisponible]) VALUES (%s,%s,%s,%s,%s,%s,%s)" %(f_estid,wrap_and_encode(f_estdisp),f_bicdisp,f_num,wrap_and_encode(' '.join(f_lugar.split(' '))),f_anclajetot,f_ancdisp))
        sqltext =("INSERT INTO estado ([estacionid],[estaciondisponible],[bicicletasdisponibles],[numero],[anclajedisponible]) VALUES (%s,%s,%s,%s,%s)" %(f_estid,wrap_and_encode(f_estdisp),f_bicdisp,f_num,f_ancdisp))
        #bla=f_estid+trenn+' '.join(f_estnom.split(' '))+trenn+f_bicdisp+trenn+f_estdisp+trenn+f_num+trenn+' '.join(f_lugar.split(' '))+trenn+f_anclajetot+trenn+f_ancdisp
        print(sqltext)
        cursor.execute(sqltext)
        #writer.writerow([bla])
        cnxn.commit()

#myfile.close()
#with open(filename1, 'w') as file_:
#    file_.write(strXML)

