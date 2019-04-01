#Für die exakte Zeitangabe muss ein UNIX-Timestamp ermittelt werden
import datetime
import time
import pytz
#Webscraping von javascript Elementen
#Installiere geckodriver und lege den Pfad in der Umgebungsvariable Path ab, wenn firefox der browser ist.
#Bei Chrome muss ein anderer Treiber unterm Pfad abgelegt werden. 
#Siehe auch https://pythonspot.com/selenium-webdriver/
from selenium import webdriver
from pyvirtualdisplay import Display
import pandas as pd
from sqlalchemy import create_engine
sqlengine=create_engine('postgres://postgres:HEJllyxlgKgCLr7l@127.0.0.1:5433/SmartGrid')
def strom_Preis():
    #Aktuelle Uhrzeit (Umwandlung in deutsche Zeit)
    datetimetuple = datetime.datetime.now(pytz.timezone('Europe/Berlin'))
    #Erstellen eines neuen datetime-Objekts, in welchem Minuten und Sekunden genullt sind
    datetime_fullhour = datetime.datetime(  
        datetimetuple.year,
        datetimetuple.month,
        datetimetuple.day,
        datetimetuple.hour)
    link_Stunde = str(int(time.mktime(datetime_fullhour.timetuple())))+'000'
    #Linkaufbau mithilfe Variable aktuelle_Stunde
    link = 'https://www.smard.de/home/marktdaten/78?marketDataAttributes=%7B%22resolution%22:%22hour%22,%22region%22:%22DE%22,%22from%22:'+link_Stunde+',%22to%22:'+link_Stunde+',%22moduleIds%22:%5B1000100,8004169%5D,%22selectedCate
gory%22:8,%22activeChart%22:false%7D'
    display = Display(visible=0, size=(800, 600))
    display.start()
    driver = webdriver.Firefox()
    driver.implicitly_wait(10)
    driver.get(link)
    #Finde die Tabelle mit dem Strompreis
    strom_Element = driver.find_element_by_id(id_='discreteTable')
    #Trage den Strompreis in ein Dictonary
    strom_Preis = {strom_Element.find_elements_by_xpath('.//th')[3].text.replace(",",":") :
     [strom_Element.find_element_by_xpath('.//td').text.replace(",",".")]}
    driver.close()
    display.stop()
    #print(strom_Preis)
    return strom_Preis
first = True
tages_Werte = dict()
while True:
    if datetime.datetime.now(pytz.timezone('Europe/Berlin')).minute == 0 and first == True:
        first = False
        tages_Werte.update(strom_Preis())
        df=pd.DataFrame.from_dict(tages_Werte,orient='index',columns=["act_price"])
        df=df.reset_index()
        print(df)
        df["act_price"] = pd.to_numeric(df["act_price"])
        df["time"] = pd.to_datetime(df["index"], format='%d.%m.%Y: %H:%M').dt.strftime('%Y-%m-%d %H:%M:%S')
        df.to_sql(name='stromdaten_tab',index=True,con=sqlengine, if_exists='append')
        tages_Werte.clear()
    elif datetime.datetime.now(pytz.timezone('Europe/Berlin')).minute != 0:
        first = True
        time.sleep(30)
    else:
        next