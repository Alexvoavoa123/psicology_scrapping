# valores_dict = {"Empresa" : [{"Psicologa 1" : {"Nombre de los psicologos" : "Pepe,Manuel,Juan","Numero de psicologos" : 4 }},{"Psicologa 1" : {"Nombre de los psicologos" : "Pepe,Manuel,Juan","Numero de psicologos" : 4 }}]}



# print(valores_dict["Empresa"][0]["Psicologa 1"])

from requests_html import HTMLSession
import pandas as pd
from bs4 import BeautifulSoup
import re
from unidecode import unidecode
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
# Se queda---------------------------
start = time.perf_counter()

# df_mujeres = pd.read_csv(r"C:\Users\Cash\Downloads\spanish-names-master\spanish-names-master\mujeres.csv")
# df_hombres = pd.read_csv(r"C:\Users\Cash\Downloads\spanish-names-master\spanish-names-master\hombres.csv")


df_mujeres = pd.read_csv(r"mujeres.csv")
df_hombres = pd.read_csv(r"hombres.csv")

df = pd.concat([df_mujeres, df_hombres])

#-------------------------------------


def seleccionar(x):
    valores_xpl =  ["UNA","SABER","CITA","HE","TOMA","YA","IRA","LE","SALUD","MAYO","LEE","CITA","SALUD","VISITA","HA","HAN","MIRA","AREA","ENERO","FEBRERO","MARZO","PASION","PASIÓN","FELICIDAD"]
    if x in valores_xpl:
        return True

indice = df.loc[df["nombre"].apply(seleccionar) == True].index
df.drop(index=indice,inplace=True)

def insertar(x):
    if "http" not in str(x):
        return f"https://{x}"
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

palabras = ["team", "somos", "equipo", "personal", "nosotros", "contacto", "nosotras", "contacta"]


valores_dict = {"Empresa": []}
url_definitivas = {}
respuestas_http = {}



# Función que extrae URLs que contienen palabras clave

def contiene_palabra(value, palabras,url):
    """Verificar si una palabra clave está en el texto."""
    for palabra in palabras:
        if re.search(palabra, value, re.IGNORECASE) and "#" not in value and "http" in value:
            return value
        elif re.search(palabra, value, re.IGNORECASE) and "#" not in value and "http" not in value:
            return f"{url}{value}"
        # ----------------------------------------------------------------------------
        # if re.search(palabra, value, re.IGNORECASE) and "http" in value:
        #     return value
        # elif re.search(palabra, value, re.IGNORECASE) and "http" not in value:
        #     return f"{url}{value}"
  
    return None


def extract_urls(i, df_listado, url_definitivas, respuestas_http):
    try:
        url = df_listado.iloc[i]["URL del sitio web"]
        id_df = df_listado.iloc[i]["ID de registro"]
        session = HTMLSession()
        response = session.get(url)
        respuestas_http[str(id_df)] = response.text
        soup = BeautifulSoup(response.text, "lxml")
        data = {a["href"] for a in soup.find_all("a", href=True)}
        
        data = list(data)
        valores = [contiene_palabra(value, palabras, url) for value in data if contiene_palabra(value, palabras, url)] + [url]
        url_definitivas[str(id_df)] = valores
        # print(valores)
        # print(data)
    except Exception as e:
        print(f"Error en extract_urls: {e}")

# Función que filtra resultados y busca nombres, números y correos electrónicos
def fetch_html(data_urls,responses_data):
    try:

        id_valor = data_urls[0]
        valores = set()
        numeros = set()
        correo_electrónico = set()
        fundador = set()

        # print(data_urls[1])

        for data in data_urls[1]:
      
            session = HTMLSession()
            response = session.get(data)
            soup = BeautifulSoup(response.text, "lxml")

            resultados = [tag.get_text() for tag in soup.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'li', "a","span","header","href"])]


           
            for resultado in resultados:
                if unidecode(re.findall(r"[\S]*", resultado)[0].upper()) in df["nombre"].values and re.findall(r"[\S]*", resultado)[0].capitalize() not in ",".join(list(valores)):

                    valores.add(resultado.replace("\n", ""))
                    # if "fundador" in resultado or "director" in resultado:
                    #     # fundador.add(f"{re.findall(r"[\S]*", resultado)[0]} {re.findall(r"[\S]*", resultado)[1]}")
                    #     fundador.add(resultado)

                elif re.findall(r'\+34{1,3}[\S\s]{1,30}', resultado):
                    numeros.add(resultado.replace("\n", ""))
                elif len(re.findall(r'\d', resultado)) == 9 and (re.findall(r'\d', resultado)[0] == "6" or re.findall(r'\d', resultado)[0] == "9"):
                    numeros.add("".join(re.findall(r'\d', resultado)))
                elif "@" in resultado and len(resultado.split()) == 1:
                    correo_electrónico.add(resultado.replace("\n", "").strip())

        
                    


        data_dict_final = {
            id_valor: {
                "Nombres de los psicologos": list(valores),  # Convertir a lista
                "Número de psicologos": list(numeros),  # Convertir a lista
                "Correo electrónico": list(correo_electrónico),  # Convertir a lista
                "Cantidad": len(valores),
                "Fundador/director" : list(fundador)
            }
        }


        #  or re.findall(r"[\S]*", value)[-1].lower() in data_dict_final[id_valor]["Correo electrónico"][0]
        for value in data_dict_final[id_valor]["Nombres de los psicologos"]:
    
            if re.findall(r"[\S]*", value)[0].lower() in data_dict_final[id_valor]["Correo electrónico"][0] or "fundador" in value or "director" in value or data_dict_final[id_valor]["Cantidad"] == 1:
                data_dict_final[id_valor]["Fundador/director"].append(value)

        

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
def async_spider(dataframe):

    df_listado = pd.read_excel(dataframe)

    df_listado = df_listado.loc[df_listado["URL del sitio web"].isna() == False]

    df_listado["URL del sitio web"] = df_listado["URL del sitio web"].apply(insertar)



    number_rows = len(df_listado["URL del sitio web"].values)
    counter = 0
  
    with ThreadPoolExecutor(max_workers=20) as executor:  # Define el número de hilos
        while counter < number_rows:
            futures = []
            # Ejecutar extract_urls en paralelo
            for i in range(counter, min(counter + 100, number_rows)):
                futures.append(executor.submit(extract_urls, i, df_listado, url_definitivas, respuestas_http))

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

    with ThreadPoolExecutor(max_workers=20) as executor:
        futures = []
        for key,url in data_urls.items():
            futures.append(executor.submit(fetch_html, (key,url),responses_data))

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
    

    data_keysss = [list(data.keys())[0] for data in def_data["Empresa"]]

    final_dict = {}

    for i,data in zip(data_keysss,def_data["Empresa"]):
        final_dict[i] = data[i]


    def traer_data_json(row):

        if str(row["ID de registro"]) in data_keysss:
            
            return final_dict[str(row["ID de registro"])]["Nombres de los psicologos"],final_dict[str(row["ID de registro"])]["Número de psicologos"],final_dict[str(row["ID de registro"])]["Correo electrónico"],final_dict[str(row["ID de registro"])]["Cantidad"],final_dict[str(row["ID de registro"])]["Fundador/director"]
        else:
            return "No hay valores","No hay valores","No hay valores","No hay valores","No hay valores"
    
    df_listado[["Nombres de los psicologos","Número de psicologos","Correo electrónico","Cantidad","Fundador"]] = pd.DataFrame(df_listado.apply(traer_data_json, axis=1).tolist(), index=df_listado.index)
    df_listado["Nombres de los psicologos"] = df_listado["Nombres de los psicologos"].fillna("").astype(str)
    df_listado["Número de psicologos"] = df_listado["Número de psicologos"].fillna("").astype(str)
    df_listado["Correo electrónico"] = df_listado["Correo electrónico"].fillna("").astype(str)
    df_listado["Fundador"] = df_listado["Fundador"].fillna("").astype(str)

    df_listado["Nombres de los psicologos"] = df_listado["Nombres de los psicologos"].str.replace(r"[\[\]']", "", regex=True)
    df_listado["Número de psicologos"] = df_listado["Número de psicologos"].str.replace(r"[\[\]']", "", regex=True)
    df_listado["Correo electrónico"] = df_listado["Correo electrónico"].str.replace(r"[\[\]']", "", regex=True)
    df_listado["Fundador"] = df_listado["Fundador"].str.replace(r"[\[\]']", "", regex=True)
    # df_listado["Fundador"] = df_listado.apply(palabras_buscar,axis=1)
    
    
    return df_listado




    
