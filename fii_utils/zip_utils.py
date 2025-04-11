"""
Utilitários para manipulação de arquivos ZIP.
Centraliza funções relacionadas à extração e verificação de arquivos ZIP.
"""

import os
import logging
import zipfile
import time
from typing import List, Optional, Tuple, Set, Dict

from fii_utils.logging_manager import get_logger


def normalizar_nome_arquivo(nome_arquivo: str) -> Tuple[str, str]:
    """
    Normaliza o nome de um arquivo e identifica a extensão.
    
    Args:
        nome_arquivo: Nome do arquivo (com extensão)
        
    Returns:
        Tuple (nome_base, extensao)
    """
    # Converte para maiúsculas para garantir consistência
    nome_arquivo = nome_arquivo.upper()
    
    # Identifica a extensão
    if nome_arquivo.endswith('.ZIP'):
        return nome_arquivo[:-4], '.ZIP'
    elif nome_arquivo.endswith('.TXT'):
        return nome_arquivo[:-4], '.TXT'
    else:
        # Se não tem extensão reconhecida, retorna nome original e extensão vazia
        return nome_arquivo, ''


def obter_caminho_arquivo_correspondente(caminho_arquivo: str, extensao_destino: str) -> str:
    """
    Dado um caminho de arquivo, retorna o caminho para a versão correspondente 
    com a extensão especificada.
    
    Args:
        caminho_arquivo: Caminho completo do arquivo original
        extensao_destino: Extensão desejada ('.ZIP' ou '.TXT')
        
    Returns:
        Caminho do arquivo correspondente
    """
    # Obtém o diretório e o nome do arquivo
    diretorio = os.path.dirname(caminho_arquivo)
    nome_arquivo = os.path.basename(caminho_arquivo)
    
    # Normaliza o nome e obtém a extensão
    nome_base, _ = normalizar_nome_arquivo(nome_arquivo)
    
    # Retorna o caminho com a nova extensão
    return os.path.join(diretorio, nome_base + extensao_destino)


def extrair_zip(zip_path: str, extract_to: Optional[str] = None, 
                max_retries: int = 3, retry_delay: float = 2.0) -> List[str]:
    """
    Extrai um arquivo ZIP com suporte a múltiplas tentativas.
    
    Args:
        zip_path: Caminho completo para o arquivo ZIP
        extract_to: Diretório para extração (padrão: mesmo diretório do ZIP)
        max_retries: Número máximo de tentativas em caso de falha
        retry_delay: Tempo de espera entre tentativas (segundos)
        
    Returns:
        Lista de caminhos completos para os arquivos extraídos ou lista vazia em caso de falha
    """
    logger = get_logger('FIIDatabase')

    # Verifica se o arquivo existe
    if not os.path.exists(zip_path):
        logger.error(f"Arquivo ZIP não encontrado: {zip_path}")
        return []
    
    # Define o diretório de extração
    if extract_to is None:
        extract_to = os.path.dirname(zip_path)
    
    # Garante que o diretório de extração existe
    os.makedirs(extract_to, exist_ok=True)
    
    # Tenta extrair o arquivo com múltiplas tentativas
    for tentativa in range(max_retries):
        try:
            extracted_files = []
            
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Lista de arquivos no ZIP
                file_list = zip_ref.namelist()
                logger.info(f"Tentativa {tentativa+1}/{max_retries}: Extraindo {len(file_list)} arquivos de {zip_path}")
                
                # Extrai todos os arquivos
                for file in file_list:
                    zip_ref.extract(file, extract_to)
                    extracted_path = os.path.join(extract_to, file)
                    extracted_files.append(extracted_path)
                    logger.info(f"Arquivo extraído: {file}")
            
            # Extração bem-sucedida
            return extracted_files
            
        except zipfile.BadZipFile:
            logger.error(f"Arquivo ZIP inválido: {zip_path}")
            # Problemas com o formato do arquivo são fatais, não tente novamente
            return []
            
        except Exception as e:
            logger.error(f"Erro ao extrair {zip_path} (tentativa {tentativa+1}/{max_retries}): {e}")
            
            if tentativa < max_retries - 1:
                logger.info(f"Aguardando {retry_delay} segundos antes da próxima tentativa...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Todas as {max_retries} tentativas de extração falharam para {zip_path}")
                return []
    
    # Não deveria chegar aqui, mas por segurança
    return []


def obter_arquivos_processados_do_banco(db_path: str, logger: logging.Logger) -> Set[str]:
    """
    Consulta o banco de dados para obter a lista de arquivos ZIP já processados.
    
    Args:
        db_path: Caminho para o arquivo do banco de dados
        logger: Logger para registro de eventos
        
    Returns:
        Conjunto de nomes de arquivos ZIP já processados
    """
    try:
        # Importação tardia para evitar problemas de importação circular
        from db_managers.arquivos import ArquivosProcessadosManager
        
        # Consultar o banco para determinar quais ZIPs já foram processados
        arquivos_processados = set()
        arquivos_manager = ArquivosProcessadosManager(db_path)
        
        try:
            arquivos_manager.conectar()
            
            # Obter lista de arquivos já processados
            arquivos = arquivos_manager.listar_arquivos_processados()
            for a in arquivos:
                if a['nome_arquivo'].endswith('.ZIP'):
                    arquivos_processados.add(a['nome_arquivo'])
                    
            logger.info(f"Encontrados {len(arquivos_processados)} arquivos ZIP já processados no banco")
            
        finally:
            if arquivos_manager and hasattr(arquivos_manager, 'fechar_conexao'):
                arquivos_manager.fechar_conexao()
            
        return arquivos_processados
        
    except Exception as e:
        logger.error(f"Erro ao consultar banco de dados: {e}")
        return set()


def verificar_extrair_zips_pendentes(diretorio: str, logger: logging.Logger, 
                                    arquivos_processados: Set[str],
                                    config: Dict) -> Tuple[int, int]:
    """
    Verifica e extrai arquivos ZIP pendentes em um diretório.
    Um ZIP é considerado pendente se não estiver na lista de arquivos processados,
    independentemente da existência do TXT correspondente.
    
    Args:
        diretorio: Diretório onde buscar arquivos ZIP
        logger: Logger para registro de eventos
        arquivos_processados: Conjunto de nomes de arquivos ZIP já processados
        config: Configuração com parâmetros de extração
        
    Returns:
        Tupla (processados, falhas) com o número de arquivos processados e falhas
    """
    logger.info(f"Verificando arquivos ZIP pendentes em {diretorio}...")
    
    # Parâmetros de configuração para extração
    max_retries = config.get("extract_retries", 3)
    retry_delay = config.get("extract_retry_delay", 2.0)
    
    # Contadores
    processados = 0
    falhas = 0
    zips_pendentes = []
    
    # Procura todos os arquivos ZIP no diretório
    for nome_arquivo in os.listdir(diretorio):
        nome_arquivo = nome_arquivo.upper()  # Normaliza para maiúsculas
        
        if nome_arquivo.endswith('.ZIP') and nome_arquivo.startswith('COTAHIST_'):
            # Verifica se o ZIP já foi processado (único critério)
            if nome_arquivo in arquivos_processados:
                logger.debug(f"Arquivo ZIP {nome_arquivo} já processado. Ignorando.")
                continue
            
            # Adicionar à lista de ZIPs pendentes
            zip_path = os.path.join(diretorio, nome_arquivo)
            zips_pendentes.append(zip_path)
    
    # Processa os ZIPs pendentes
    if zips_pendentes:
        logger.info(f"Encontrados {len(zips_pendentes)} ZIPs pendentes para extração")
        
        for zip_path in zips_pendentes:
            nome_arquivo = os.path.basename(zip_path)
            logger.info(f"Extraindo ZIP pendente: {nome_arquivo}")
            
            # Tenta extrair o arquivo
            extracted_files = extrair_zip(
                zip_path, diretorio,
                max_retries=max_retries,
                retry_delay=retry_delay
            )
            
            if extracted_files:
                processados += 1
                logger.info(f"ZIP pendente extraído com sucesso: {nome_arquivo}")
            else:
                falhas += 1
                logger.error(f"Falha ao extrair ZIP pendente: {nome_arquivo}")
    else:
        logger.info("Nenhum ZIP pendente encontrado para extração")
    
    # Resumo
    logger.info(f"Verificação de ZIPs pendentes concluída. Processados: {processados}, Falhas: {falhas}")
    return processados, falhas