import tweepy
from datetime import datetime, timedelta, timezone
import mysql.connector
import json
import pickle
import pandas as pd
import re
import pytz


model_gempa = pickle.load(open(r'model\clf.pkl','rb'))
model_banjir = pickle.load(open(r'model\clf_banjir.pkl','rb'))
tfidf_gempa = pickle.load(open(r'model\tfidf1.pkl', 'rb'))
tfidf_banjir = pickle.load(open(r'model\tfidf1_banjir.pkl', 'rb'))

mydb = mysql.connector.connect(
  host="",
  user="",
  passwd="",
  database="")

class StreamListener(tweepy.Stream):

    def on_status (self, status):
        if len(self.tweets) == self.limit:
            self.disconnect()
   

    def on_data(self, data):
        all_data = json.loads(data)
        text = all_data['text']
        created_at = all_data['created_at']
        converted = pytz.timezone('Asia/Bangkok')
        created_at = datetime.strptime(created_at, '%a %b %d %H:%M:%S %z %Y')
        created_at = created_at.astimezone(converted)
        created_at = created_at.strftime('%a %b %d %H:%M:%S %z %Y')
        id = all_data['id']
        if all_data['place'] == None:
          place = None
          location = None
          bounding_box = None
          coordinates = None
          lon,lat = [None,None]
        else:
          place = all_data['place']
          location = place['full_name']
          bounding_box = place['bounding_box']
          coordinates = bounding_box['coordinates']
          lon,lat = coordinates[0][0]
        username = all_data['user']['screen_name']
        if username == "infoBMKG":
            print(text)
            if "Mag:" in text :
                mag = re.findall(r'Mag:(\d+.?\d*)', text)
                mag = float(mag[0])
            elif "Magnitudo:" in text:
                mag = re.findall(r'Magnitudo: (\d+.?\d*)', text)
                mag = float(mag[0])
            BT = re.findall(r'(\d+.?\d*) BT', text)
            re_longitude = float(BT[0])
            if "LU" in text:
                LU = re.findall(r'(\d+.?\d*) LU',text)
                re_latitude = float(LU[0])
            elif "LS" in text:
                LS = re.findall(r'(\d+.?\d*) LS',text)
                re_latitude = float(LS[0])*-1
            mycursor = mydb.cursor()
            mycursor.execute("INSERT INTO bmkg (text,username, re_longitude, re_latitude, created_at, mag) VALUES (%s, %s, %s, %s, %s, %s)",(text,username,re_longitude,re_latitude,created_at,mag))
            mydb.commit()
        if "gempa" in text.lower():
            predict = model_gempa.predict(tfidf_gempa.transform([text]))
            all_data['predicted'] = int(predict)
            predicted = all_data['predicted']
            mycursor = mydb.cursor()
            mycursor.execute("INSERT INTO gempa (id, username, text, created_at, location, predicted, lon, lat) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(id,username,text,created_at,location,predicted,lon,lat))
            mydb.commit()
        elif "banjir" in text.lower():
            predict = model_banjir.predict(tfidf_banjir.transform([text]))
            all_data['predicted'] = int(predict)
            predicted = all_data['predicted']
            mycursor = mydb.cursor()
            mycursor.execute("INSERT INTO banjir (id, username, text, created_at, location, predicted, lon, lat) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",(id,username,text,created_at,location,predicted,lon,lat))
            mydb.commit()



stream_listener = StreamListener('tSTGheSxnBYdbeAsdgoONHpKO',
        '6olnst42blgg7SieQDZc0JNXrcfPhefgyzt2Thleg2K1qJ86BP',
        '1488430474758598658-ZwqvC9nBE5vw9TXGa0qpvm2VeTMzSo',
        'TsTy0apy2aCzEuXBj7p0W2VRG3ucr0HBXfLmirQGHyioI')
stream_listener.filter(track = ["gempa","banjir"])