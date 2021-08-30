import sqlite3

conn = sqlite3.connect("/home/sedov/repos/4upwork/mydb.db")
cursor = conn.cursor()

# table creation
cursor.execute("""CREATE TABLE files
                  (source_file_path text, source_file_format text, target_file_path text,
                   target_file_format text, status text)
               """)

files = [('/home/sedov/repos/4upwork/test_files/file1.csv', 'csv',
          '/home/sedov/repos/4upwork/test_files/file1_csv2json.json', 'json', 'P'),
         ('/home/sedov/repos/4upwork/test_files/file2.json', 'json',
          '/home/sedov/repos/4upwork/test_files/file2_json2csv.csv', 'csv', 'P'),
         ('/home/sedov/repos/4upwork/test_files/file3.csv', 'csv',
          '/home/sedov/repos/4upwork/test_files/file3_csv2csv.csv', 'csv', 'P'),
         ('/home/sedov/repos/4upwork/test_files/file4.json', 'json',
          '/home/sedov/repos/4upwork/test_files/file4_json2json.json', 'json', 'P')
         ]

cursor.executemany("INSERT INTO files VALUES (?,?,?,?,?)", files)
conn.commit()
conn.close()

