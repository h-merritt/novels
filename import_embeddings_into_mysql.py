import sqlite3
import json


print("importing vectors for entire corpus")

cnx = sqlite3.connect('vectors.db')
cnx.text_factory = str
cur = cnx.cursor()
cur.execute('DROP TABLE IF EXISTS coming_of_age;')
cur.execute('CREATE TABLE coming_of_age (word VARCHAR(255), vector TEXT, PRIMARY KEY (word));')

with open("vectors.txt", 'r') as f:
    jobs = 0
    for line in f:
        row = line.rstrip().split(' ')
        word = str(row[0])
        if len(word) <= 255:
            vector = [float(val) for val in row[1:]]
            s_vector = json.dumps(vector)
            sql = 'INSERT OR IGNORE INTO {} (word, vector) VALUES (?, ?)'.format(
                'coming_of_age')
            insert_data = (word, s_vector)
            cur.execute(sql, insert_data)
            jobs += 1
            if jobs % 1000000 == 0:
                print('Jobs done: {0}'.format(jobs))
        else:
            print('one exclusion: ', word)

cnx.commit()
cur.execute('SELECT count(word), count(vector) FROM {};'.format('coming_of_age'))
print(cur.fetchall())
cnx.close()


print("importing vectors for corpus with books 1920 - 1950")

cnx1920to1950 = sqlite3.connect('vectors_1920to1950.db')
cnx1920to1950.text_factory = str
cur1920to1950 = cnx1920to1950.cursor()
cur1920to1950.execute('DROP TABLE IF EXISTS era1920to1950;')
cur1920to1950.execute('CREATE TABLE era1920to1950 (word VARCHAR(255), vector TEXT, PRIMARY KEY (word));')

with open("vectors_1920to1950.txt", 'r') as f:
    jobs = 0
    for line in f:
        row = line.rstrip().split(' ')
        word = str(row[0])
        if len(word) <= 255:
            vector = [float(val) for val in row[1:]]
            s_vector = json.dumps(vector)
            sql = 'INSERT OR IGNORE INTO {} (word, vector) VALUES (?, ?)'.format(
                'era1920to1950')
            insert_data = (word, s_vector)
            cur1920to1950.execute(sql, insert_data)
            jobs += 1
            if jobs % 1000000 == 0:
                print('Jobs done: {0}'.format(jobs))
        else:
            print('one exclusion: ', word)

cnx1920to1950.commit()
cur1920to1950.execute('SELECT count(word), count(vector) FROM {};'.format('era1920to1950'))
print(cur1920to1950.fetchall())
cnx1920to1950.close()


print('importing vectors for corpus with books 1950 - 1981')

cnx1951to1981 = sqlite3.connect('vectors_1951to1981.db')
cnx1951to1981.text_factory = str
cur1951to1981 = cnx1951to1981.cursor()
cur1951to1981.execute('DROP TABLE IF EXISTS era1951to1981;')
cur1951to1981.execute('CREATE TABLE era1951to1981 (word VARCHAR(255), vector TEXT, PRIMARY KEY (word));')

with open("vectors_1951to1981.txt", 'r') as f:
    jobs = 0
    for line in f:
        row = line.rstrip().split(' ')
        word = str(row[0])
        if len(word) <= 255:
            vector = [float(val) for val in row[1:]]
            s_vector = json.dumps(vector)
            sql = 'INSERT OR IGNORE INTO {} (word, vector) VALUES (?, ?)'.format(
                'era1951to1981')
            insert_data = (word, s_vector)
            cur1951to1981.execute(sql, insert_data)
            jobs += 1
            if jobs % 1000000 == 0:
                print('Jobs done: {0}'.format(jobs))
        else:
            print('one exclusion: ', word)

cnx1951to1981.commit()
cur1951to1981.execute('SELECT count(word), count(vector) FROM {};'.format('era1951to1981'))
print(cur1951to1981.fetchall())
cnx1951to1981.close()


print('importing vectors for corpus with books 1982 - 2012')

cnx1982to2012 = sqlite3.connect('vectors_1982to2012.db')
cnx1982to2012.text_factory = str
cur1982to2012 = cnx1982to2012.cursor()
cur1982to2012.execute('DROP TABLE IF EXISTS era1982to2012;')
cur1982to2012.execute('CREATE TABLE era1982to2012 (word VARCHAR(255), vector TEXT, PRIMARY KEY (word));')

with open("vectors_1982to2012.txt", 'r') as f:
    jobs = 0
    for line in f:
        row = line.rstrip().split(' ')
        word = str(row[0])
        if len(word) <= 255:
            vector = [float(val) for val in row[1:]]
            s_vector = json.dumps(vector)
            sql = 'INSERT OR IGNORE INTO {} (word, vector) VALUES (?, ?)'.format(
                'era1982to2012')
            insert_data = (word, s_vector)
            cur1982to2012.execute(sql, insert_data)
            jobs += 1
            if jobs % 1000000 == 0:
                print('Jobs done: {0}'.format(jobs))
        else:
            print('one exclusion: ', word)

cnx1982to2012.commit()
cur1982to2012.execute('SELECT count(word), count(vector) FROM {};'.format('era1982to2012'))
print(cur1982to2012.fetchall())
cnx1982to2012.close()


print('importing vectors for corpus with books 2013 - 2022')

cnx2013to2022 = sqlite3.connect('vectors_2013to2022.db')
cnx2013to2022.text_factory = str
cur2013to2022 = cnx2013to2022.cursor()
cur2013to2022.execute('DROP TABLE IF EXISTS era2013to2022;')
cur2013to2022.execute('CREATE TABLE era2013to2022 (word VARCHAR(255), vector TEXT, PRIMARY KEY (word));')

with open("vectors_2013to2022.txt", 'r') as f:
    jobs = 0
    for line in f:
        row = line.rstrip().split(' ')
        word = str(row[0])
        if len(word) <= 255:
            vector = [float(val) for val in row[1:]]
            s_vector = json.dumps(vector)
            sql = 'INSERT OR IGNORE INTO {} (word, vector) VALUES (?, ?)'.format(
                'era2013to2022')
            insert_data = (word, s_vector)
            cur2013to2022.execute(sql, insert_data)
            jobs += 1
            if jobs % 1000000 == 0:
                print('Jobs done: {0}'.format(jobs))
        else:
            print('one exclusion: ', word)

cnx2013to2022.commit()
cur2013to2022.execute('SELECT count(word), count(vector) FROM {};'.format('era2013to2022'))
print(cur2013to2022.fetchall())
cnx2013to2022.close()

print('finished')
