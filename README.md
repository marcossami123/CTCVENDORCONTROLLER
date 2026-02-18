Proceso de auditoria ICBC

1.
-El proceso comienza corriendo ShowHTML_ICBC.py, que se encuentra dentro de la carpeta de ShowHTML.
-El código toma el último archivo de ProductosPatagonia que se encuentra en DataStorage, identificando al VENDOR a través del ProviderID que se encuentra tanto en ProductosPatagonia como en el archivo de icbc_vendors.py que esta dentro de la carpeta config.
-Dada la informacion, crea la URL:

"https://mall.icbc.com.ar/buscar"
"?controller=search"
"&orderby=outstanding"
"&orderway=desc"
"&search_query={vendor}"

En donde todo es fijo salvo el nombre del VENDOR, que extrae tambien del icbc_vendors.py
-Se accede a esa URL, que deberia contener todos los productos publicados del VENDOR, y se descarga el HTML de todas y cada una de las paginas con productos, iterando hasta llegar a la ultima
-Se guarda un archivo con los resultados llamado "Nombre del vendor_ICBC_DataLayerRaw_fechahora"
(Ej:diggit_ICBC_DataLayerRaw_20260129_093729.csv) dentro de DataStorage

2.
- Se ejecuta HTML_Price_Parser_ICBC.py, que se encuentra dentro de la carpeta HTML_Price_Parser
- Este proceso extrae el SKU y el Precio De Venta del ultimo archivo DataLayerRaw (que se genero en el paso previo) y se guarda el Data Set dentro de DataStorage

3.
- Se ejecuta el PriceComparisonICBC.py que se encuentra dentro de la carpeta de Audit. Este proceso toma el archivo con los precios parseados en el paso previo y los compara con los del archivo de PreciosPatagonia, matcheandolos a través del SKU. Es importante considerar que al hacer la descarga del HTML se baja la info de TODOS los productos que tiene publicado un VENDOR en el catalogo, pero puede ser que no no tenga publicados ninguno de los que esta en ProductosPatagonia, por lo que no habra matcheo y no generará archivo de auditoria.

Proceso Auditoria BNA

1. Se ejecuta el codigo api_caller_BNA.py, que esta dentro de la carpeta BNA. Este proceso consulta la API de BNA, dandole como parametro el Shop ID que es el numero de tienda del VENDOR en BNA, actuando como identificador ÚNICO (similar a nuestro ProviderId). El llamado se hace utilizando el Shop ID correspondiente al VENDOR que este siendo auditado en el ultimo ProductosPatagonia. Recordemos que el mismo tiene el ProviderID, y matchea con el icbc_vendors.py.py declarado dentro de config. Se guarda un CSV en DataStorage con el formato BNA_Prices_VENDOR_fechahora.csv (Ej BNA_Prices_diggit_20260129_093837.csv), el cual contiene el SKU y el precio de BNA. Los datos de la llamada estan en bna_api_config.py dentro de config

2. Se ejecuta el archivo PriceComparisonBNA.py, guardado dentro de la carpeta Audit, el cual realiza la comparacion de precios entre los productos, matcheandolos por SKU. Si bien este proceso funciona a través de una API, al igual que ICBC, se descarga la informacion para TODOS los productos que tiene el VENDOR publicados, matcheandolos luego por SKU a la hora de auditar, pero podria pasar que ninguno de los productos que se están usando para la auditoria en ProductosPatagonia esté publicado en BNA, por lo que no se generaria el archivo.




