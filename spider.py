from requests_html import HTMLSession
import pandas as pd
from bs4 import BeautifulSoup
import re
from unidecode import unidecode
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib3
from requests.exceptions import SSLError

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# Se queda---------------------------
start = time.perf_counter()

# df_mujeres = pd.read_csv(r"C:\Users\Cash\Downloads\spanish-names-master\spanish-names-master\mujeres.csv")
# df_hombres = pd.read_csv(r"C:\Users\Cash\Downloads\spanish-names-master\spanish-names-master\hombres.csv")


df_mujeres = pd.read_csv(r"mujeres.csv")
df_hombres = pd.read_csv(r"hombres.csv")

df = pd.concat([df_mujeres, df_hombres])

#-------------------------------------


def seleccionar(x):
    valores_xpl =  ["UNA","SABER","CITA","HE","TOMA","YA","IRA","LE","SALUD","MAYO","LEE","CITA","SALUD","VISITA","HA","HAN","MIRA","AREA","ENERO","FEBRERO","MARZO","PASION","PASIÓN","FELICIDAD","PRESENTACION","PROCESO"]
    if x in valores_xpl:
        return True

indice = df.loc[df["nombre"].apply(seleccionar) == True].index
df.drop(index=indice,inplace=True)

def insertar(x):
    if "http" not in str(x):
        return f"http://{x}"
    else:
        return x

def quitar_barra(x):
    if x[-1] == "/":
        return x[:-1]
    else:
        return x






# def palabras_buscar(row):
#     try:

#         valores = re.findall(r"[\S]*",row["Nombre de la empresa"])
#         if pd.isna(row["Fundador"])==True and unidecode(valores[0].upper()) in df["nombre"].values:
#             return valores[0]
#         elif row["Fundador"] == "No hay valores" and unidecode(valores[0].upper()) in df["nombre"].values:
#             return valores[0]
#         else:
#             return row["Fundador"]
#     except:
#         pass


names_value = []
numbers_value = []
email_value = []
quantity = []

palabras = ["team", "somos", "equip", "personal", "nosotros", "contact", "nosotras", "contacta","sobre","quienes","qui","ekipamendua"]


valores_dict = {"Empresa": []}
url_definitivas = {}
respuestas_http = {}



session = HTMLSession()

# Función que extrae URLs que contienen palabras clave

def contiene_palabra(value, palabras,url):
    """Verificar si una palabra clave está en el texto."""
    for palabra in palabras:
        if re.search(palabra, value, re.IGNORECASE) and "#contacto" not in value and "http" in value:
            return value
        elif re.search(palabra, value, re.IGNORECASE) and "#contacto" not in value and "http" not in value:
            return f"{url}{value}"
        # ----------------------------------------------------------------------------
        # if re.search(palabra, value, re.IGNORECASE) and "http" in value:
        #     return value
        # elif re.search(palabra, value, re.IGNORECASE) and "http" not in value:
        #     return f"{url}{value}"
  
    return None


def extract_urls(i, df_listado, url_definitivas, respuestas_http,session):
    try:
        url = df_listado.iloc[i]["URL del sitio web"]
        id_df = df_listado.iloc[i]["ID de registro"]
        # session = HTMLSession()
        try:
            response = session.get(url)

        except SSLError:
            
            print(f"SSL error en {url}, intentando sin verificación de SSL.")
            response = session.get(url, verify=False)

        # response = session.get(url)
        respuestas_http[str(id_df)] = response.text
        soup = BeautifulSoup(response.text, "lxml")
        data = {a["href"] for a in soup.find_all("a", href=True)}
        
        data = list(data)
        valores_xc = [contiene_palabra(value, palabras, url) for value in data if contiene_palabra(value, palabras, url)] + [url]
        url_definitivas[str(id_df)] = valores_xc
        # print(valores)
        # print(data)
    except Exception as e:
        print(f"Error en extract_urls: {e}")

def safe_href_or_text(tag):
    
    data_value = []

    try:
        if tag.attrs["href"]:
            data_value.append(tag.attrs["href"])

        data.append(tag.get_text())

        return " ".join(data_value)
    except:
        return tag.get_text()



# Función que filtra resultados y busca nombres, números y correos electrónicos
def fetch_html(data_urls,responses_data,session):

    try:

        id_valor = data_urls[0]
        valores = set()
        numeros = set()
        correo_electrónico = set()
        fundador = set()

        # print(data_urls[1])

        for data in data_urls[1]:
      
            # session = HTMLSession()

            data_first_part = data[:8]
            data_last_part = data[8:]

            data_last_part = data_last_part.replace("//", "/")

            data = data_first_part + data_last_part

            try:
                response = session.get(data, verify=False) 
            except SSLError:
                print(f"SSL error en {data}, intentando sin verificación de SSL.")
                response = session.get(data, verify=False)

            soup = BeautifulSoup(response.text, "lxml")

            # Manera 1

            resultados = [tag.get_text() for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', "a","span","header"])]
            
            # ----------------------------------------------------------------------------------------------------------------

            # Manera 2
            # pattern = re.compile(r"(slid|opini)", re.IGNORECASE)

            # resultados = [tag for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', "a","span","header"])]

            # resultado_valor = [valor.get_text() for valor in resultados if not pattern.search(str(valor))]

            # --------------------------------------------------------------------------------------------------------------

            # Manera 3

            # resultados = [div.get_text() for div in soup.find_all(['div','p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', "a","span","header"]) if div.get('class') and not any("slid" in cls for cls in div.get('class'))]

            for resultado in resultados:
                

                try:

                    # print(valores)

                
                    if unidecode(re.findall(r"[\S]*", resultado)[0].upper()) in df["nombre"].values and re.findall(r"[\S]*", resultado)[0].capitalize() not in ",".join(list(valores)):

                        valores.add(resultado.replace("\n", ""))
                        # print(resultado)

                    elif re.findall(r'\+34{1,3}[\S\s]{1,30}', resultado):
                        numeros.add(resultado.replace("\n", ""))
                    elif len(re.findall(r'\d', resultado)) == 9 and (re.findall(r'\d', resultado)[0] == "6" or re.findall(r'\d', resultado)[0] == "9"):
                        numeros.add("".join(re.findall(r'\d', resultado)))
                        if "@" in resultado:
                            correo_electrónico.add(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', resultado)[0].replace("\n", "").strip())

                    elif re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', resultado):
                        correo_electrónico.add(re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', resultado)[0].replace("\n", "").strip())
                    # elif "@" in resultado and len(resultado.split()) == 1:
                    #     correo_electrónico.add(resultado.replace("\n", "").strip())

                except:
                    pass
                    # if data == "https://www.luzarango.com/":

                    #     print(resultado)

            
        
                    





        # data_dict_final = {}

        # id_valor = {}
    

        # try:
        #     id_valor: ["Nombres de los psicologos"] = list(valores)
   
        # except:
        #     id_valor: ["Nombres de los psicologos"] = list()
        # # ----------------------------------------------

        # try:

        #     id_valor: ["Número de psicologos"] = list(numeros)  # Convertir a lista
            
        # except:
        #     id_valor: ["Número de psicologos"] = list()

        # # ----------------------------------------------

        # try:
        #     id_valor["Correo electrónico"] = list(correo_electrónico)  # Convertir a lista
    
        # except:
        #     id_valor["Correo electrónico"] = list()

        # # ----------------------------------------------

        # try:
        #     id_valor["Cantidad"] = len(valores) # Convertir a lista
           
        # except:
        #     id_valor["Cantidad"] = 0
        
        # # ----------------------------------------------

        # try:
        #     id_valor["Fundador/director"] = list(fundador) # Convertir a lista
            
        # except:
        #     id_valor["Fundador/director"] = list()

        # # ----------------------------------------------


        # data_dict_final[data_urls[0]] = id_valor

        # id_valor: {
        #         "Nombres de los psicologos": list(valores),  # Convertir a lista
        #         "Número de psicologos": list(numeros),  # Convertir a lista
        #         "Correo electrónico": list(correo_electrónico),  # Convertir a lista
        #         "Cantidad": len(valores),
        #         "Fundador/director" : list(fundador)
        #     }

        # data_dict_final["id_valor"] = id_valor

        try:


            data_dict_final = {
                id_valor: {
                    "Nombres de los psicologos": list(valores),  # Convertir a lista
                    "Número de psicologos": list(numeros),  # Convertir a lista
                    "Correo electrónico": list(correo_electrónico),  # Convertir a lista
                    "Cantidad": len(valores),
                    "Fundador/director" : list(fundador)
                }
            }

            

        except:
            pass
        


        #  or re.findall(r"[\S]*", value)[-1].lower() in data_dict_final[id_valor]["Correo electrónico"][0]
        for value in data_dict_final[id_valor]["Nombres de los psicologos"]:
            try:

        
                if re.findall(r"[\S]*", value)[0].lower() in data_dict_final[id_valor]["Correo electrónico"][0] or "fundador" in value or "director" in value or "direcci" in value or data_dict_final[id_valor]["Cantidad"] == 1:
                    data_dict_final[id_valor]["Fundador/director"].append(value)
            
            except:
                pass

        print(data_dict_final)

        

            # elif len(re.findall(r"[\S]*", value)) > 1:
            #     if value.split()[-1] in data_dict_final[id_valor]["Correo electrónico"][0]:
            #         print(value.split()[-1])
        
        valores_dict["Empresa"].append(data_dict_final)
        # print(f"""
        # Valores : {valores},
        # Numeros : {numeros},
        # Correo electrónico : {correo_electrónico}
        # Quantity : {len(valores)}
        # ------------------------------------
        # """)

    except Exception as e:
        print(f"Error en fetch_html: {e}")

# Implementación de ThreadPoolExecutor
# def clean_data(dataframe):

#     df_listado = pd.read_excel(dataframe)

#     df_listado = df_listado.loc[df_listado["URL del sitio web"].isna() == False]

#     df_listado["URL del sitio web"] = df_listado["URL del sitio web"].apply(insertar)

#     return df_listado

def async_spider(dataframe):

    df_listado = pd.read_excel(dataframe,sheet_name="Hoja1")

    # df_listado = df_listado.loc[df_listado["URL del sitio web"].isna() == False]

    df_listado["URL del sitio web"] = df_listado["URL del sitio web"].apply(insertar)

    # --------------------------
    # df_listado = dataframe

    number_rows = len(df_listado["URL del sitio web"].values)
    # number_rows = 1500
    counter = 0
  
    with ThreadPoolExecutor(max_workers=10) as executor:  # Define el número de hilos
        while counter < number_rows:
            futures = []
            # Ejecutar extract_urls en paralelo
            for i in range(counter, min(counter + 100, number_rows)):
                futures.append(executor.submit(extract_urls, i, df_listado, url_definitivas, respuestas_http,session))

            # Esperar a que todas las tareas terminen
            for future in as_completed(futures):
                try:
                    future.result()  # Lanza cualquier excepción ocurrida en el hilo
                except Exception as e:
                    print(f"Error en hilo extract_urls: {e}")

            counter += 100
            print(f"Progreso actual: {counter}")

    

    

    # with open(r"C:\Users\Cash\Desktop\portfolio project\urls_threadpool.json", "w",encoding="UTF-8") as j:
    #     json.dump(url_definitivas, j, ensure_ascii=False, indent=4)

    # # with open(r"C:\Users\Cash\Desktop\portfolio project\urls_reponse_text.json", "w",encoding="UTF-8") as j:
    # #     json.dump(respuestas_http, j, ensure_ascii=False, indent=4)

    # # Fetch data---------------------------------------------------------------------------------------------------------

    # with open(r"C:\Users\Cash\Desktop\portfolio project\urls_threadpool.json") as j:
    data_urls = url_definitivas

    # with open(r"C:\Users\Cash\Desktop\portfolio project\urls_reponse_text.json",encoding="UTF-8") as j:
    responses_data = respuestas_http

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for key,url in data_urls.items():
            futures.append(executor.submit(fetch_html, (key,url),responses_data,session))

        for future in as_completed(futures):
            try:
                future.result()  
            except Exception as e:
                print(f"Error en hilo fetch_html: {e}")

    # with open(r"C:\Users\Cash\Desktop\portfolio project\valor_final.json","w") as j:
    #     json.dump(valores_dict,j)

    # Last step-----------------------------------------------------------------------------------

    # with open(r"C:\Users\Cash\Desktop\portfolio project\valor_final.json") as j:
    def_data= valores_dict

    print(valores_dict)
    

    data_keysss = [list(data.keys())[0] for data in def_data["Empresa"]]

    final_dict = {}

    for i,data in zip(data_keysss,def_data["Empresa"]):
        final_dict[i] = data[i]


    def traer_data_json(row):

        if str(row["ID de registro"]) in data_keysss:
            
            return final_dict[str(row["ID de registro"])]["Nombres de los psicologos"],final_dict[str(row["ID de registro"])]["Número de psicologos"],final_dict[str(row["ID de registro"])]["Correo electrónico"],final_dict[str(row["ID de registro"])]["Cantidad"],final_dict[str(row["ID de registro"])]["Fundador/director"]
        else:
            return "No hay valores","No hay valores","No hay valores","No hay valores","No hay valores"

    def criterio_fundador(row):
        try:

            if row["Cantidad"] == 1 and row["Fundador"] in [None, ""]:
                return row["Nombres de los psicologos"]
            
            # Si el nombre de la empresa coincide con el nombre de una persona y el fundador está vacío
            nombre_empresa = re.findall(r"[\S]+", row["Nombre de la empresa"])
            print(nombre_empresa)
            primer_nombre_empresa = nombre_empresa[0].capitalize()
            segundo_nombre_empresa = nombre_empresa[1].capitalize()
            if unidecode(primer_nombre_empresa.upper()) in df["nombre"].values and row["Fundador"] in [None, ""]:
                return f"{primer_nombre_empresa} {segundo_nombre_empresa}"
            
            # Si no se cumplen las condiciones, devolver el valor original de "Fundador"
            return row["Fundador"]
    
        except Exception as e:
            print(f"Error en criterio_fundador: {e}")
            return row["Fundador"]

    def limpiar_datos(row):
        try:
            final_data = row["Fundador"].title()
            final_data2 = final_data.replace("\t","").replace("\T","")
            final_data3 = re.findall(r"[\S]+", final_data2)
            final_data4 = f"{final_data3[0]} {final_data3[1]}"
            data_x1 = re.findall(r"[\S]+",row["Nombre de la empresa"])
            if final_data3[0] ==  data_x1[0]:
                return f"{final_data3[0]} {data_x1[1]}"
        
           
                
            return final_data4
            # if row[]



        except:
            pass
    def limpieza_final(row):
        try:
            if pd.isna(row["Fundador"]) == True:
                value = re.findall(r'\b(?!Psic)\w+\b', row["Nombre de la empresa"])
                value2 = re.findall(r"[\S]+",row["Nombres de los psicologos"])
                # if (value[0].lower() in row["Nombres de los psicologos"].lower() and value[0].upper() in df["nombre"].values) or (value[0].lower() in row["Correo electrónico"] and value[0].upper() in df["nombre"].values):
                if value[0].lower() in row["Nombres de los psicologos"].lower() or value[0].lower() in row["Correo electrónico"]:
                    return f"{value[0]} {value[1]}"

                elif value2[1].lower() in row["URL del sitio web"].lower() or value2[1].lower() in row["Correo electrónico"]:
                    return f"{value2[0]} {value2[1]}"
            else:
                return row["Fundador"]
                # value = re.findall(r'\b(?!Psic)\w+\b', row["Nombre de la empresa"])
                # value2 = re.findall(r"[\S]+",row["Nombres de los psicologos"])
                # if value[0] in row["Nombres de los psicologos"] or value[0] in row["Correo electrónico"]:
                #     return f"{value[0]} {value[1]}"

                # elif value2[1] in row["URL del sitio web"] or value2[1] in row["Correo electrónico"]:
                #     return f"{value2[0]} {value2[1]}"


        except:
            pass
    
    df_listado[["Nombres de los psicologos","Número de psicologos","Correo electrónico","Cantidad","Fundador"]] = pd.DataFrame(df_listado.apply(traer_data_json, axis=1).tolist(), index=df_listado.index)
    df_listado["Nombres de los psicologos"] = df_listado["Nombres de los psicologos"].fillna("").astype(str)
    df_listado["Número de psicologos"] = df_listado["Número de psicologos"].fillna("").astype(str)
    df_listado["Correo electrónico"] = df_listado["Correo electrónico"].fillna("").astype(str)
    df_listado["Fundador"] = df_listado["Fundador"].fillna("").astype(str)

    df_listado["Nombres de los psicologos"] = df_listado["Nombres de los psicologos"].str.replace(r"[\[\]']", "", regex=True)
    df_listado["Número de psicologos"] = df_listado["Número de psicologos"].str.replace(r"[\[\]']", "", regex=True)
    df_listado["Correo electrónico"] = df_listado["Correo electrónico"].str.replace(r"[\[\]']", "", regex=True)
    df_listado["Fundador"] = df_listado["Fundador"].str.replace(r"[\[\]']", "", regex=True)
    df_listado["Fundador"] = df_listado.apply(criterio_fundador,axis=1)
    df_listado["Fundador"] = df_listado.apply(limpiar_datos,axis=1)
    df_listado["Fundador"] = df_listado.apply(limpieza_final,axis=1)
    
    
    return df_listado


# if __name__=="__main__":
  
#     # df_data = clean_data(r"C:\Users\Cash\Downloads\psicologia_leads_HS_fiverr.xlsx")
#     df = async_spider(r"C:\Users\Cash\Downloads\testing_psicologia_21_32.xlsx")
#     df.to_excel(r"C:\Users\Cash\Downloads\final_dataaaaaaaaa.xlsx")
    

    
