#Loading required libaries

import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

#Reading and parsing Avro Schema File
schema = avro.schema.Parse(open('sample.avsc', "r").read())

#Printing Avro Schema
print(schema)

#Creating a empty Avro file using Avro schema
writer = DataFileWriter(open("sample.avro", "wb"), DatumWriter(), schema)

#Writning data to Avro file using Avro schema

writer.append({"customer_id": 123, "customer_name":'abc', "joining_date":20120131,"salary":4000.50          ,"address":"D.No 1234 , ABC , abc , ABC"
              ,"updated_date":1538265652000})
writer.close()

#Reading Avro file

reader = DataFileReader(open("sample.avro", "rb"), DatumReader())

#Printing Avro file data
for user in reader:
    print (user)
reader.close()
