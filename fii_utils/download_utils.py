"""
Utilitários para download de arquivos da B3.
Centraliza funções relacionadas ao download de arquivos históricos.
"""

import os
import logging
import calendar
import datetime
import traceback
from typing import List, Tuple, Dict, Optional, Any

# Importações de outros módulos do sistema
from fii_utils.downloader import (
    verificar_arquivo_disponivel, baixar_com_fallback, 
    baixar_arquivos_diarios as baixar_arquivos_diarios_original,
    baixar_arquivos_mensais as baixar_arquivos_mensais_original,
    baixar_arquivos_anuais as baixar_arquivos_anuais_original
)
from fii_utils.zip_utils import (
    verificar_extrair_zips_pendentes,
    obter_arquivos_processados_do_banco
)


def baixar_arquivo_diario(dia: str, mes: str, ano: str, config: Dict, force: bool = False) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Baixa um arquivo diário para uma data específica.
    
    Args:
        dia: Dia (string de 2 dígitos)
        mes: Mês (string de 2 dígitos)
        ano: Ano (string de 4 dígitos)
        config: Configuração
        force: Se deve forçar o download mesmo se já existir
        
    Returns:
        Tupla (sucesso, zip_path, txt_path)
    """
    logger = logging.getLogger('FIIDatabase')
    logger.info(f"Tentando baixar arquivo diário para {dia}/{mes}/{ano}")
    
    try:
        # Verificar se o arquivo já existe localmente
        arquivo_zip = os.path.join(config["data_dir"], f"COTAHIST_D{dia}{mes}{ano}.ZIP")
        arquivo_txt = os.path.join(config["data_dir"], f"COTAHIST_D{dia}{mes}{ano}.TXT")
        
        if os.path.exists(arquivo_txt) and not force:
            logger.info(f"Arquivo diário para {dia}/{mes}/{ano} já existe localmente. Pulando download.")
            return True, arquivo_zip, arquivo_txt
        
        # Verificar se está disponível no servidor
        disponivel, _ = verificar_arquivo_disponivel("daily", dia, mes, ano, config)
        
        if disponivel:
            logger.info(f"Baixando arquivo diário para {dia}/{mes}/{ano}...")
            status, zip_path, txt_path = baixar_com_fallback(dia, mes, ano, config, force)
            
            if status == "success" and txt_path:
                logger.info(f"Download do arquivo diário para {dia}/{mes}/{ano} concluído com sucesso.")
                return True, zip_path, txt_path
            else:
                logger.error(f"Falha ao baixar arquivo diário para {dia}/{mes}/{ano}.")
                return False, None, None
        else:
            logger.info(f"Arquivo diário para {dia}/{mes}/{ano} não disponível no servidor.")
            return False, None, None
            
    except Exception as e:
        logger.error(f"Erro ao baixar arquivo diário para {dia}/{mes}/{ano}: {e}")
        logger.error(traceback.format_exc())
        return False, None, None


def baixar_arquivo_mensal(mes: str, ano: str, config: Dict, force: bool = False) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Baixa um arquivo mensal para um mês e ano específicos.
    
    Args:
        mes: Mês (string de 2 dígitos)
        ano: Ano (string de 4 dígitos)
        config: Configuração
        force: Se deve forçar o download mesmo se já existir
        
    Returns:
        Tupla (sucesso, zip_path, txt_path)
    """
    logger = logging.getLogger('FIIDatabase')
    logger.info(f"Tentando baixar arquivo mensal para {mes}/{ano}")
    
    try:
        # Verificar se o arquivo já existe localmente
        arquivo_zip = os.path.join(config["data_dir"], f"COTAHIST_M{mes}{ano}.ZIP")
        arquivo_txt = os.path.join(config["data_dir"], f"COTAHIST_M{mes}{ano}.TXT")
        
        if os.path.exists(arquivo_txt) and not force:
            logger.info(f"Arquivo mensal para {mes}/{ano} já existe localmente. Pulando download.")
            return True, arquivo_zip, arquivo_txt
        
        # Verificar se está disponível no servidor
        disponivel, _ = verificar_arquivo_disponivel("monthly", None, mes, ano, config)
        
        if disponivel:
            # Determinar o último dia do mês para o download
            ultimo_dia = str(calendar.monthrange(int(ano), int(mes))[1]).zfill(2)
            
            logger.info(f"Baixando arquivo mensal para {mes}/{ano}...")
            status, zip_path, txt_path = baixar_com_fallback(ultimo_dia, mes, ano, config, force)
            
            if status == "success" and txt_path:
                logger.info(f"Download do arquivo mensal para {mes}/{ano} concluído com sucesso.")
                return True, zip_path, txt_path
            else:
                logger.error(f"Falha ao baixar arquivo mensal para {mes}/{ano}.")
                return False, None, None
        else:
            logger.info(f"Arquivo mensal para {mes}/{ano} não disponível no servidor.")
            return False, None, None
            
    except Exception as e:
        logger.error(f"Erro ao baixar arquivo mensal para {mes}/{ano}: {e}")
        logger.error(traceback.format_exc())
        return False, None, None


def baixar_arquivo_anual(ano: str, config: Dict, force: bool = False) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Baixa um arquivo anual para um ano específico.
    
    Args:
        ano: Ano (string de 4 dígitos)
        config: Configuração
        force: Se deve forçar o download mesmo se já existir
        
    Returns:
        Tupla (sucesso, zip_path, txt_path)
    """
    logger = logging.getLogger('FIIDatabase')
    logger.info(f"Tentando baixar arquivo anual para {ano}")
    
    try:
        # Verificar se o arquivo já existe localmente
        arquivo_zip = os.path.join(config["data_dir"], f"COTAHIST_A{ano}.ZIP")
        arquivo_txt = os.path.join(config["data_dir"], f"COTAHIST_A{ano}.TXT")
        
        if os.path.exists(arquivo_txt) and not force:
            logger.info(f"Arquivo anual para {ano} já existe localmente. Pulando download.")
            return True, arquivo_zip, arquivo_txt
        
        # Verificar se está disponível no servidor
        disponivel, _ = verificar_arquivo_disponivel("yearly", None, None, ano, config)
        
        if disponivel:
            logger.info(f"Baixando arquivo anual para {ano}...")
            status, zip_path, txt_path = baixar_com_fallback("31", "12", ano, config, force)
            
            if status == "success" and txt_path:
                logger.info(f"Download do arquivo anual para {ano} concluído com sucesso.")
                return True, zip_path, txt_path
            else:
                logger.error(f"Falha ao baixar arquivo anual para {ano}.")
                return False, None, None
        else:
            logger.info(f"Arquivo anual para {ano} não disponível no servidor.")
            return False, None, None
            
    except Exception as e:
        logger.error(f"Erro ao baixar arquivo anual para {ano}: {e}")
        logger.error(traceback.format_exc())
        return False, None, None


def baixar_arquivo(periodo: str, params: Dict[str, str], config: Dict, force: bool = False) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    Função unificada para baixar arquivos de qualquer período.
    
    Args:
        periodo: Tipo de período ("daily", "monthly" ou "yearly")
        params: Parâmetros específicos para o período (dia, mes, ano)
        config: Configuração
        force: Se deve forçar o download mesmo se já existir
        
    Returns:
        Tupla (sucesso, zip_path, txt_path)
    """
    if periodo == "daily":
        return baixar_arquivo_diario(params["dia"], params["mes"], params["ano"], config, force)
    elif periodo == "monthly":
        return baixar_arquivo_mensal(params["mes"], params["ano"], config, force)
    elif periodo == "yearly":
        return baixar_arquivo_anual(params["ano"], config, force)
    else:
        raise ValueError(f"Período inválido: {periodo}")


def baixar_arquivos_diarios(data_inicio: datetime.datetime, data_fim: datetime.datetime, config: Dict, force: bool = False) -> List[Tuple[str, str, str]]:
    """
    Wrapper para a função original baixar_arquivos_diarios.
    
    Args:
        data_inicio: Data inicial
        data_fim: Data final
        config: Configuração
        force: Se deve forçar o download mesmo se já existir
        
    Returns:
        Lista de tuplas (dia, mes, ano) baixados com sucesso
    """
    logger = logging.getLogger('FIIDatabase')
    logger.info(f"Baixando arquivos diários de {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")
    
    return baixar_arquivos_diarios_original(data_inicio, data_fim, config, force)


def baixar_arquivos_mensais(mes_inicio: int, ano_inicio: int, mes_fim: int, ano_fim: int, config: Dict, force: bool = False) -> List[Tuple[str, str]]:
    """
    Wrapper para a função original baixar_arquivos_mensais.
    
    Args:
        mes_inicio: Mês inicial (1-12)
        ano_inicio: Ano inicial
        mes_fim: Mês final (1-12) 
        ano_fim: Ano final
        config: Configuração
        force: Se deve forçar o download mesmo se já existir
        
    Returns:
        Lista de tuplas (mes, ano) baixados com sucesso
    """
    logger = logging.getLogger('FIIDatabase')
    logger.info(f"Baixando arquivos mensais de {mes_inicio}/{ano_inicio} a {mes_fim}/{ano_fim}")
    
    return baixar_arquivos_mensais_original(mes_inicio, ano_inicio, mes_fim, ano_fim, config, force)


def baixar_arquivos_anuais(ano_inicio: int, ano_fim: int, config: Dict, force: bool = False) -> List[str]:
    """
    Wrapper para a função original baixar_arquivos_anuais.
    
    Args:
        ano_inicio: Ano inicial
        ano_fim: Ano final
        config: Configuração
        force: Se deve forçar o download mesmo se já existir
        
    Returns:
        Lista de anos baixados com sucesso
    """
    logger = logging.getLogger('FIIDatabase')
    logger.info(f"Baixando arquivos anuais de {ano_inicio} a {ano_fim}")
    
    return baixar_arquivos_anuais_original(ano_inicio, ano_fim, config, force)


def baixar_arquivos_auto(args: Any, config: Dict, db_path: str, logger: logging.Logger) -> bool:
    """
    Determina automaticamente quais arquivos baixar com base no estado do banco.
    
    Args:
        args: Argumentos da linha de comando
        config: Configuração
        db_path: Caminho para o arquivo do banco de dados
        logger: Logger para registro de eventos
        
    Returns:
        bool: True se bem-sucedido, False caso contrário
    """
    from db_managers.arquivos import ArquivosProcessadosManager
    from fii_utils.downloader import determinar_arquivos_para_baixar, baixar_multiplos_arquivos
    
    try:
        # Instanciar gerenciador de arquivos para verificar o último processado
        arquivos_manager = ArquivosProcessadosManager(db_path)
        
        try:
            # Determinar quais arquivos baixar
            arquivos_manager.conectar()
            datas = determinar_arquivos_para_baixar(arquivos_manager)
            arquivos_manager.fechar_conexao()
            
            # Se não houver datas para baixar, retorna sucesso
            if not datas:
                logger.info("Nenhuma data para baixar automaticamente")
                return True
            
            # Baixar os arquivos
            logger.info(f"Baixando automaticamente {len(datas)} arquivos")
            sucessos, falhas, nao_disponiveis, arquivos_txt = baixar_multiplos_arquivos(datas, False)
            
            # Verificar e extrair ZIPs pendentes após os downloads
            arquivos_processados = obter_arquivos_processados_do_banco(db_path, logger)
            verificar_extrair_zips_pendentes(config["data_dir"], logger, arquivos_processados, config)
            
            # Retorna sucesso se pelo menos um arquivo foi baixado
            return sucessos > 0
            
        finally:
            # Garantir que a conexão é fechada
            if hasattr(arquivos_manager, 'fechar_conexao'):
                arquivos_manager.fechar_conexao()
                
    except Exception as e:
        logger.error(f"Erro ao baixar arquivos automaticamente: {e}")
        logger.error(traceback.format_exc())
        return False