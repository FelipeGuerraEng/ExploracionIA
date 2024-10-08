import os
import pdb
import json
import logging
from tqdm import tqdm
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import ResourceNotFoundError 
from dotenv import load_dotenv, find_dotenv

# Buscar y cargar automáticamente el archivo .env
load_dotenv(find_dotenv())

logger = logging.getLogger('logger_app.blob')

class BlobFunctions:
    """
    La clase BlobFunctions está diseñada para facilitar la interacción con Azure Blob Storage, ofreciendo métodos para cargar,
    descargar y almacenar archivos y datos. Esta clase se encarga de la inicialización automática de la conexión con Azure Blob Storage
    utilizando credenciales definidas en un archivo de variables de entorno.
    """

    def __init__(self, container_name=None):

        # Utiliza DefaultAzureCredential para autenticarse automáticamente
        credential = DefaultAzureCredential()

        # Inicia el BlobServiceClient con DefaultAzureCredential
        # account_url = f"https://{os.getenv('AZURE_BLOB_STORAGE_BLOB_NAME')}.blob.core.windows.net"
        account_url = "https://datalakecomfama.blob.core.windows.net"
        self.blob_service_client = BlobServiceClient(account_url=account_url, credential=credential)

        if not container_name:
            # Lee el nombre del contenedor desde las variables de entorno
            self.container_name = os.getenv("AZURE_STORAGE_CONTAINER_NAME")
        else:
            self.container_name = container_name

        self.container_client = self.blob_service_client.get_container_client(self.container_name)

    def listar_archivos_mas_grandes_por_carpeta(self):
        """
        Lista los archivos más grandes en cada carpeta dentro del contenedor de Azure Blob Storage.
        Devuelve una lista con las rutas de estos archivos.
        No lista carpetas vacías ni blobs que no sean archivos.
        """
        dir_largest_blob = {}  # Diccionario para almacenar el blob más grande por directorio
        blobs = self.container_client.list_blobs()

        for blob in blobs:
            # Obtener la ruta del blob
            blob_path_parts = blob.name.split('/')
            directory = '/'.join(blob_path_parts[:-1])  # Ruta de la carpeta (puede ser vacía para root)

            # Verificar si es un archivo, ignorando "carpetas"
            if not blob.name.endswith('/'):
                # Obtener el tamaño del blob
                blob_size = blob.size if hasattr(blob, 'size') else blob.properties.content_length

                # Verificar si este blob es el más grande en su directorio
                if directory not in dir_largest_blob or blob_size > dir_largest_blob[directory]['size']:
                    dir_largest_blob[directory] = {'name': blob.name, 'size': blob_size}  # Actualizar con el blob más grande

        # Construir la lista de rutas de los blobs más grandes
        largest_files_paths = []
        for directory, blob_info in dir_largest_blob.items():
            if blob_info['size'] > 0:  # Verificar que el archivo no esté vacío
                largest_files_paths.append(blob_info['name'])
                print(f"Archivo más grande en '{directory}': {blob_info['name']} (tamaño: {blob_info['size']} bytes)")
                # logger.info(f"Archivo más grande en '{directory}': {blob_info['name']} (tamaño: {blob_info['size']} bytes)")

        return largest_files_paths



    def extract_file_from_blob(self, blob_folder_path=None, end=None, blob_list=None):
        """
        Descarga archivos desde una carpeta específica o desde una lista de blobs en Azure Blob Storage.

        :param blob_folder_path: Ruta de la carpeta dentro del contenedor desde donde descargar los archivos.
        :param end: Extensión de archivo o cadena final para filtrar los archivos a descargar (solo usado si no se pasa `blob_list`).
        :param blob_list: Lista de rutas de blobs específicas a descargar. Si se proporciona, se ignora `blob_folder_path` y `end`.
        :return: Lista de diccionarios con los nombres de archivos y su contenido en bytes.
        """
        if blob_list is None:
            # Si no se proporciona una lista de blobs, listar blobs desde la carpeta especificada
            if not blob_folder_path:
                raise ValueError("Debe proporcionarse una ruta de carpeta o una lista de blobs.")
            
            print(f"\nIniciando carga de archivos desde: \n {blob_folder_path}")
            logger.info(f"\nIniciando carga de archivos desde: \n {blob_folder_path}")

            # Listar blobs que empiecen con el prefijo de la carpeta
            blob_list = [
                blob.name
                for blob in self.container_client.list_blobs(
                    name_starts_with=f"{blob_folder_path}/"
                )
            ]

            # Filtrar por extensión (si se proporciona)
            if end:
                blob_list = [blob for blob in blob_list if blob.endswith(end)]
        
        else:
            # Si se proporciona una lista de blobs específicos
            print("\nIniciando carga de archivos desde la lista de blobs proporcionada.")
            logger.info("\nIniciando carga de archivos desde la lista de blobs proporcionada.")

        files_list = []

        # Descargar cada archivo desde el Blob Storage
        for blob_name in tqdm(blob_list):
            logger.info(f"Loading {blob_name}")
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_name
            )
            download_stream = blob_client.download_blob()
            file_bytes = download_stream.readall()

            files_list.append(
                {
                    "file_name": blob_name.split("/")[-1],  # Asume que el nombre del archivo está después del último '/'
                    "file": file_bytes,
                }
            )

        return files_list


    def save_json_to_blob(self, blob_folder_path, file_name, content, verbose=False):
        """
        Guarda contenido en un archivo dentro de Azure Blob Storage.

        :param blob_folder_path: Ruta de la carpeta dentro del contenedor donde se guardará el archivo.
        :param file_name: Nombre del archivo a guardar.
        :param content: Contenido del archivo a guardar.
        """
        if verbose:
            print(f"Iniciando guardado de archivo en {blob_folder_path}/{file_name}")

        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name, blob=f"{blob_folder_path}/{file_name}"
        )

        content_as_string = json.dumps(content, ensure_ascii=False, indent=4)

        blob_client.upload_blob(content_as_string, overwrite=True)
        if verbose:
            print(f"Archivo {file_name} guardado en {blob_folder_path}/")
        logger.info(f"Archivo {file_name} guardado en {blob_folder_path}/")
    
    def upload_blob(self, file_name, dir_destiny, file_path=None, file_obj=None):
        """
        Sube un archivo al Azure Blob Storage, desde una ruta local o directamente desde un objeto en memoria.

        :param file_name: Nombre del archivo a subir.
        :param dir_destiny: Directorio de destino dentro del contenedor de Azure Blob Storage.
        :param file_path: Ruta local del archivo a subir (usada si file_obj no se proporciona).
        :param file_obj: Objeto de archivo en memoria para subir (usado si se proporciona, ignorando file_path).
        """
        blob_full_path = os.path.join(dir_destiny, file_name)

        if file_obj is not None:
            # Subir desde un objeto de archivo en memoria
            print(f"\nSubiendo archivo desde el objeto en memoria a {blob_full_path}")
            logger.info(f"\nSubiendo archivo desde el objeto en memoria a {blob_full_path}")
            self.container_client.upload_blob(
                name=blob_full_path, data=file_obj, overwrite=True
            )
        elif file_path is not None:
            # Subir desde una ruta local
            print(f"Subiendo archivo desde la ruta local {file_path} a {blob_full_path}")
            logger.info(f"Subiendo archivo desde la ruta local {file_path} a {blob_full_path}")
            with open(file_path, "rb") as data:
                self.container_client.upload_blob(
                    name=blob_full_path, data=data, overwrite=True
                )
        else:
            logger.warning("Debe proporcionarse file_path o file_obj.")
            raise ValueError("Debe proporcionarse file_path o file_obj.")

        print(f"Archivo subido con éxito como {blob_full_path} al contenedor {self.container_name}.")
        logger.info(f"Archivo subido con éxito como {blob_full_path} al contenedor {self.container_name}.")

    def calcular_tamano_total_blobs(self, blob_list):
        """
        Calcula los tamaños de los blobs en la lista proporcionada y los devuelve en kilobytes.

        :param blob_list: Lista de rutas de blobs específicas.
        :return: Lista de tamaños de los blobs en kilobytes (KB).
        """
        sizes_in_kb = []

        for blob_name in tqdm(blob_list, desc="Calculando tamaño de blobs en kilobytes"):
            blob_client = self.blob_service_client.get_blob_client(
                container=self.container_name, blob=blob_name
            )

            try:
                # Obtener las propiedades del blob, que incluyen el tamaño
                blob_properties = blob_client.get_blob_properties()
                blob_size_kb = blob_properties.size / 1024  # Convertir tamaño de bytes a kilobytes
                sizes_in_kb.append(blob_size_kb)

            except ResourceNotFoundError:
                print(f"Error: El blob '{blob_name}' no existe o no se pudo encontrar.")
                logger.error(f"Error: El blob '{blob_name}' no existe o no se pudo encontrar.")
                sizes_in_kb.append(None)  # Para indicar que este blob no fue encontrado

        print(f"Lista de tamaños de los blobs en KB: {sizes_in_kb}")
        logger.info(f"Lista de tamaños de los blobs en KB: {sizes_in_kb}")

        return sizes_in_kb

