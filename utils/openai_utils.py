from langchain_openai import AzureChatOpenAI
from langchain_core.messages import HumanMessage, SystemMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.prompts import ChatPromptTemplate, HumanMessagePromptTemplate, SystemMessagePromptTemplate, PromptTemplate
from langchain.chains import LLMChain
from langchain.output_parsers import CommaSeparatedListOutputParser, PydanticOutputParser
from langchain_core.runnables import RunnableParallel
from tqdm import tqdm
from datetime import datetime
from langchain_community.callbacks import get_openai_callback
from typing import Optional, List
from pydantic import BaseModel, Field
from utils.kv import SecretManager
import os
from dotenv import load_dotenv, find_dotenv
import time
import logging


logger = logging.getLogger('logger_app.openai')

# Cargar variables desde el archivo .env
load_dotenv(find_dotenv())


class AzureOpenAIFunctions:
    def __init__(self):

        # Primero intentar cargar las variables de entorno
        self.endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        self.api_key = os.getenv("AZURE_OPENAI_API_KEY")
        self.model_name = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME_EMBEDDINGS")
        self.model_name_gpt = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME_GPT")
        self.api_version = os.getenv("AZURE_OPENAI_API_VERSION")

        self.llm = AzureChatOpenAI(
            azure_deployment=self.model_name_gpt,
            azure_endpoint=self.endpoint,
            api_key=self.api_key,
            api_version=self.api_version,
            temperature=0.8,
        )
        
    # Función para manejar reintentos
    def invoke_with_retry(chain, inputs, retries=8, delay=2):
        for i in range(retries):
            try:
                with get_openai_callback() as cb:
                    response = chain.invoke(inputs)["text"]
                if len(response) < 1000:
                    raise ValueError("La respuesta es menor que 1000 caracteres")
                return response, cb
            except Exception as e:
                print(f"Intento {i + 1} fallido: {e}")
                time.sleep(delay)
        print(f"Fallo después de {retries} reintentos.")
        return inputs["text"], None
