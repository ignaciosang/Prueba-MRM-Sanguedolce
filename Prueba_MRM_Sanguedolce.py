#Importacion de paquetes 
import numpy as np
import pandas as pd
import pyRofex 
import yfinance as yf
from pandas.tseries.offsets import BMonthEnd
from datetime import datetime
from operator import itemgetter
import itertools
#%%
#Creo una clase "User" que representa al usuario de MatbaRofex. El unico objetivo de este objeto es lograr la conexión con la API. 
#Ademas, permite separar 

class User():
    def __init__(self,user,password,account,environment):
        self.__user = user
        self.__password = password
        self.__account = account
        self.environment = environment
        self.market_connect()
    def market_connect(self):
        pyRofex.initialize(user="jsanguedolceangelini7852", 
                           password="djtafS8*", 
                           account="REM7852",
                           environment=pyRofex.Environment.REMARKET)
    
#%%
class Futuros_Bot(User):
    def __init__(self, user, password, account, environment, tickers):
        super().__init__(user, password, account, environment)
        self.tickers = tickers
       
     
    def get_symbols(self):
        #Descargo todos los instrumentos existentes en el paquete PyRofex
        instruments = list(pyRofex.get_all_instruments().values())[1]
        #Me quedo con aquellos instrumentos derivados de los subyacentes seleccionados
        #Para ello me quedo con el valor de ["InstrumentId"]["symbol"] y separo el string en base a "/". De esta forma,
        #me permite comprobar de manera mas rapido si en cada string aparece alguno de los tickers deseados, obviando el plazo
        #Utilizando la función filter para el filtrado de las condiciones, genero una lista que contiene a aquellos que cumplen
        #las condiciones
        futuros = list(filter(lambda dic: set(bot.tickers).intersection(set(dic["instrumentId"]["symbol"].split("/"))),
                           instruments))
        #Utilizando la función json normalize, transformo el diccionario en un pandas DataFrame
        futuros = pd.json_normalize(futuros)
        #Indexo unicamente la columna correspondiente al s+imbolo del instrumento
        futuros = futuros[futuros.columns[2]]
        #Tengo varios simbolos repetidos, o distintos plazos de vencimiento colapsados en un mismo string. 
        #Utilizando las funcion map, que me permite pseudo-iterar sobre la lista aplicando una modificacion a la misma, voy a separar la lista de tickers
        #en todas las combinaciones unicas que existen con su respectivo plazo
        futuros = list(set(sum(map(lambda s: [s] if s.count('/') == 1 else [s.split('/')[0] + '/' + d for d in s.split('/')[1:]], futuros), [])))
        #futuros es una lista de tickers/símbolos de futuros que sintética 
        return futuros
    
    def get_data(self,symbols):
        #Hago request para bajar la data
        futuros_data = list(map(lambda ticker: pyRofex.get_market_data(ticker=ticker,
                                                                   entries =[pyRofex.MarketDataEntry.BIDS,pyRofex.MarketDataEntry.OFFERS]),symbols))
        #Con la funcion json normalize transformo el formato del mensaje a una lista de diccionarios                                                                            
        futuros_data_bid= list(map(lambda dic: pd.json_normalize(dic,record_path=["marketData","BI"]),futuros_data))
        #Llamo a dos funciones auxiliares que me permiten limpiar la data
        futuros_data_bid = list(map(bot.fill_empty_df_aux, futuros_data_bid))
        futuros_data_bid = list(map(lambda df: bot.add_label_aux("BID",df),futuros_data_bid))
        #Concateno todos los dfs de la lista en un unico df
        futuros_data_bid = pd.concat(futuros_data_bid)
        #Adiciono una columna con los respectivos tickers
        futuros_data_bid["Ticker"] = futuros
        
        #Aplico el mismo proceso para los BID. Podría hacerse una función auxiliar, con un input que sea "BI" o "OF"
        #y realice el proceso sin repetir codigo innecesariamente
        futuros_data_ask= list(map(lambda dic: pd.json_normalize(dic,record_path=["marketData","OF"]),futuros_data))
        futuros_data_ask = list(map(bot.fill_empty_df_aux, futuros_data_ask))
        futuros_data_ask = list(map(lambda df: bot.add_label_aux("ASK",df),futuros_data_ask))
        futuros_data_ask = pd.concat(futuros_data_ask)
        futuros_data_ask["Ticker"] = futuros
       
        futuros_data_df = pd.concat([futuros_data_ask,futuros_data_bid])
        #Agrego el ticker del subyacente para luego poder descargar los precios spots
        futuros_data_df["Subyacente"] = futuros_data_df.Ticker.str.split("/",expand=True)[0] +".BA"
        
        #La funcion devuleve el DF final
        return futuros_data
    
    def config(self,futuros_data_df):
        futuros_data_aux = futuros_data_df
        #Genero una columna con el Mes de vencimiento del contraro
        futuros_data_aux["Mes"] =futuros_data_aux.Ticker.str.split("/",expand=True)[1].str[:3]
        #Diccionario de Meses, me permite a cada mes en formato string anexarlo con un integer
        mapa_meses = {'ENE': 1, 'FEB': 2, 'MAR': 3, 'ABR': 4, 'MAY': 5, 'JUN': 6,
                     'JUL': 7, 'AGO': 8, 'SEP': 9, 'OCT': 10, 'NOV': 11, 'DIC': 12}
        #Evito elementos repetidos
        meses = list(futuros_data_aux.Mes.unique())
        #Anexo a los numeros correspondientes a cada mes
        meses_id = [mapa_meses[i] for i in meses]
        #Los contratos de futuros vencen el ultimo dia habil del mes de vencimiento.
        #Busco generar la fecha correspondiente en cada caso. 
        fechas = pd.date_range('2023-01-01', '2023-12-31', freq='BM') + pd.offsets.Day(1) - pd.offsets.BDay(1)
        last_business_days = [str(x.date()) for x in fechas if x.month in meses_id][::-1]
        maturity_dict = dict(zip(meses,last_business_days))

        #Creo columna de maturity
        futuros_data_aux['Maturity'] = futuros_data_aux['Mes'].map(maturity_dict).apply(pd.to_datetime)
        #Creo columna con la fecha de hoy
        futuros_data_aux["Today"] = datetime.today()
        #Columna Pending = Cantidad de dias entre maturity y hoy
        futuros_data_aux["Pending"] = (futuros_data_aux.Maturity- futuros_data_aux.Today)
        futuros_data_aux.Pending = futuros_data_aux.Pending.dt.days
        futuros_data_aux = futuros_data_aux.drop_duplicates()
        
        #Descargo los precios spots de los subyacentes y los anexo
        subyacentes = list(futuros_data_aux.Subyacente.unique())
        spots=yf.download(subyacentes,period= "1d")["Adj Close"].T
        spots["Subyacente"] = spots.index
        spots = spots.reset_index(drop=True)
        spots.columns = ["Spot","Subyacente"]
        futuros_data_aux = pd.merge(futuros_data_aux,spots,on="Subyacente")
        futuros_data_aux["Implicit Rate"] = ((futuros_data_aux["price"] / futuros_data_aux["Spot"])) **(360/futuros_data_aux["Pending"])

        
        
        return futuros_data_aux
    
   
        
    

    def fill_empty_df_aux(self,df):
        """Rellena un df vacío cuando el mensaje no tiene campos"""
        if df.empty: df = pd.DataFrame(data=np.zeros((1,2)),columns = ["price","size"])
        return df
    def add_label_aux(self,label,df):
        """Le agrega el label "BID" o "ASK" a la columna Type de un df" """
        df["Type"] = label
        return df
    def Arbitrage(self,futuros_data):
        #Función que itera sobre todos los posibles pares de arbitraje e imprime los que son determinados como viables
        #Agrupa el df por Mes
        for month, df_month in futuros_data.groupby('Mes'):
            #Genera dos variable,s una para ask otra para bid
            #Se van a comparar para un mismo mes, BID VS ASK
            ask_df = df_month[df_month['Type'] == 'ASK']
            bid_df = df_month[df_month['Type'] == 'BID']
            for (ask_ticker, ask_implicit), (bid_ticker, bid_implicit) in itertools.product(
                ask_df[['Ticker', 'Implicit Rate']].values, bid_df[['Ticker', 'Implicit Rate']].values
            ):
                if ask_ticker != bid_ticker and bid_implicit > ask_implicit:
                    #Si la tasa implicita dada por el BID para un instrumento a una maturity determinada es 
                    #mayor que la tasa implicita dada por el ASK de otro instrumento, hay arbitraje posible
                    print(f"Arbitraje Possible: \n BID Ticker to Buy: {bid_ticker}, \n ASK Ticker to sell: {ask_ticker},,\n BID Implicit Rate: {bid_implicit:.2f},\n ASK Implicit Rate: {ask_implicit:.2f}")

    def Main(self):
        #Funcion principalque ejecuta los distintos métodos que permiten
        #calcular las posibilidades de arbitraje
        symbols = self.get_symbols()
        futuros_data_bid, futuros_data_ask, futuros_data_df = self.get_data(symbols)
        futuros_data= self.config(futuros_data_df)
        self.Arbitrage(futuros_data)
        #El metodo imprime todos los posibles arbitrajes y returnea el df con toda la informacion
        return futuros_data
        

    
    def Real_Time(self):
        #Funcion que "Permite" correr en tiempo real la función Main. 
        #Dado que las APIs se actualizaron en DIC 2022 pero el paquete no, y no se puede
        #modificar el handler de los mensajes de suscripcion en real time para guardarlos en algun tipo 
        #de base de datos o variable,la alternativa es generar un loop que le "Pegue" o realice 
        #requests a la API. En caso de existir una cantidad limite de requests, el codigo no es funcional 
        #para periodos largos de tiempo
        #Basicamente, funciona como un invocador del metodo Main continuamente. Cuando encuentra diferencias
        #entre dos salidas consecutivas de Main, le da al usuario la opcion de detener el programa
        prev_df = self.Main()
        print(prev_dfdf)
        while True: 
            new_df = self.Main()
            if prev_df != new_df:
                prev_df = new_df
                print(new_df)
                stop = input("If you wish to stop, input anythin: ")
                if stop!="": break 
            time
    

#%%
#Iniciliazación de la clase
bot = Futuros_Bot(user="jsanguedolceangelini7852", 
                   password="djtafS8*", 
                   account="REM7852",
                   environment=pyRofex.Environment.REMARKET,tickers = ["GGAL","PAMP","YPFD"])
#Llamado a la función Main
bot.Main()



