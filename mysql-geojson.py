import mysql.connector, os, shortuuid, json, gzip, shutil
from dotenv import load_dotenv
from requests import get

def download(url, file_name): #func for download by url
    with open(file_name, "wb") as file:
        response = get(f"{url}?token={token}")
        file.write(response.content)

load_dotenv()
token = os.getenv("API_TOKEN")
links = ["https://batch.openaddresses.io/api/job/322463/output/source.geojson.gz"] #here your links

#MySQL DB properities
mydb = mysql.connector.connect(
  host=os.getenv("DB_HOST"),
  user=os.getenv("DB_USER"),
  password=os.getenv("DB_PASSWORD"))
mycursor = mydb.cursor()
mycursor.execute("USE OpenAdresses")
#mycursor.execute("DROP TABLE Adresses")

#creating table, if it doesn't exist
mycursor.execute("""
CREATE TABLE IF NOT EXISTS Adresses(
  hash VARCHAR(100), 
  number VARCHAR(100), 
  street VARCHAR(150), 
  unit VARCHAR(100), 
  city VARCHAR(50), 
  district VARCHAR(100), 
  region VARCHAR(100), 
  postcode INTEGER, 
  id VARCHAR(100),
  coordinates VARCHAR(100)
  )
  """)
mydb.commit()

for i in links:
    gzName = shortuuid.uuid()
    download(i, f"{gzName}.gz")

    #unarchive
    with gzip.open(f"{gzName}.gz", "rb") as f_in:
      with open(f"{gzName}.json", "wb") as f_out:
          shutil.copyfileobj(f_in, f_out)
          os.remove(f"{gzName}.gz")

    #json fileds reading, and appending to list
    with open(f"{gzName}.json") as f:
      data = json.loads("[" + f.read().replace("}\n{", "},\n{") + "]")
    rows = []
    for field in data:
      rows.append((
          field["properties"]["hash"],
          field["properties"]["number"],
          field["properties"]["street"],
          field["properties"]["unit"],
          field["properties"]["city"],
          field["properties"]["district"],
          field["properties"]["region"],
          field["properties"]["postcode"],
          field["properties"]["id"],
          f'{field["geometry"]["coordinates"][0]}, {field["geometry"]["coordinates"][1]}',
      ))

    #insert data to MySQL
    sql = ("INSERT IGNORE INTO Adresses (hash, number, street, unit, city, district, region, postcode, id, coordinates) VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
    mycursor.executemany(sql, rows)
    os.remove(f"{gzName}.json")

