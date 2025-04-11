"""
Sistema centralizado de logging para o Sistema de Análise de FIIs.
Fornece configuração consistente para todos os loggers do sistema e suporte a múltiplos tipos de logs.
"""

import logging
import sys
import os
from logging.handlers import RotatingFileHandler
from typing import Optional


class LoggingManager:
    """
    Gerenciador de logs que centraliza a configuração e uso de múltiplos loggers.
    Permite definir configurações comuns para todos os loggers e manter um registro
    consistente entre os diferentes componentes do sistema.
    """
    
    # Configurações padrão
    DEFAULT_LOG_DIR = 'logs'
    DEFAULT_MAX_SIZE_MB = 10
    DEFAULT_LOG_BACKUPS = 5
    DEFAULT_LOG_LEVEL = logging.INFO
    DEFAULT_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    # Dicionário para rastreamento de loggers já configurados
    _loggers = {}
    
    @classmethod
    def setup(cls, log_name: str, 
              log_dir: Optional[str] = None, 
              console: bool = True, 
              file: bool = True,
              level: int = None,
              format_str: Optional[str] = None) -> logging.Logger:
        """
        Configura e retorna um logger com o nome especificado.
        
        Args:
            log_name: Nome do logger (usado como prefixo para o arquivo de log)
            log_dir: Diretório para armazenar os logs
            console: Se True, adiciona um handler para saída no console
            file: Se True, adiciona um handler para saída em arquivo
            level: Nível de logging (se None, usa DEFAULT_LOG_LEVEL)
            format_str: String de formatação para as mensagens de log
            
        Returns:
            Logger configurado
        """
        # Se o logger já foi configurado, retorna a instância
        if log_name in cls._loggers:
            return cls._loggers[log_name]
        
        # Usa valores padrão se não especificados
        if log_dir is None:
            log_dir = cls.DEFAULT_LOG_DIR
        if level is None:
            level = cls.DEFAULT_LOG_LEVEL
        if format_str is None:
            format_str = cls.DEFAULT_FORMAT
        
        # Cria um logger personalizado
        logger = logging.getLogger(log_name)
        logger.setLevel(level)
        
        # Remove handlers existentes para evitar duplicação
        # Aqui é onde precisamos fechar os handlers antes de removê-los
        if logger.handlers:
            for handler in logger.handlers:
                # Certifique-se de fechar o handler para liberar os recursos
                handler.close()
            logger.handlers.clear()
        
        # Cria um formatador
        formatter = logging.Formatter(format_str)
        
        # Adiciona handler de console se solicitado
        if console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # Adiciona handler de arquivo se solicitado
        if file:
            # Garante que o diretório de logs existe
            os.makedirs(log_dir, exist_ok=True)
            
            # Define o nome do arquivo de log
            log_file = f"{log_name}.log"
            log_path = os.path.join(log_dir, log_file)
            
            # Cria um handler de arquivo com rotação
            file_handler = RotatingFileHandler(
                log_path,
                mode='a',
                encoding='utf-8',
                maxBytes=cls.DEFAULT_MAX_SIZE_MB * 1024 * 1024,
                backupCount=cls.DEFAULT_LOG_BACKUPS
            )
            file_handler.setLevel(level)
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        # Registra o logger no dicionário
        cls._loggers[log_name] = logger
        
        # Registra a criação do logger (exceto para o logger do próprio sistema)
        if log_name != 'system':
            cls.log_system_event(f"Logger '{log_name}' configurado com sucesso")
        
        return logger
    
    @classmethod
    def get_logger(cls, log_name: str, **kwargs) -> logging.Logger:
        """
        Retorna um logger configurado com o nome especificado.
        Se o logger ainda não existe, configura-o com os parâmetros fornecidos.
        
        Args:
            log_name: Nome do logger
            **kwargs: Argumentos adicionais para setup() se o logger não existir
            
        Returns:
            Logger configurado
        """
        # Verifica se o logger já existe
        if log_name in cls._loggers:
            return cls._loggers[log_name]
        
        # Se não existe, configura um novo
        return cls.setup(log_name, **kwargs)
    
    @classmethod
    def log_system_event(cls, message: str, level: int = logging.INFO) -> None:
        """
        Registra um evento do sistema no logger 'system'.
        
        Args:
            message: Mensagem a ser registrada
            level: Nível de logging para a mensagem
        """
        # Configura o logger do sistema se ainda não existir
        if 'system' not in cls._loggers:
            cls.setup('system', console=False)
        
        logger = cls._loggers['system']
        
        # Registra a mensagem no nível apropriado
        if level == logging.DEBUG:
            logger.debug(message)
        elif level == logging.INFO:
            logger.info(message)
        elif level == logging.WARNING:
            logger.warning(message)
        elif level == logging.ERROR:
            logger.error(message)
        elif level == logging.CRITICAL:
            logger.critical(message)
    
    @classmethod
    def setup_download_logger(cls) -> logging.Logger:
        """
        Configura e retorna um logger específico para operações de download.
        Usa configurações customizadas para essa funcionalidade específica.
        
        Returns:
            Logger configurado para operações de download
        """
        return cls.setup(
            'b3_downloader',
            console=True,
            file=True,
            level=logging.INFO
        )
    
    @classmethod
    def setup_security_logger(cls) -> logging.Logger:
        """
        Configura e retorna um logger específico para eventos de segurança.
        Este logger é mais detalhado e armazena informações sensíveis.
        
        Returns:
            Logger configurado para eventos de segurança
        """
        return cls.setup(
            'b3_security',
            console=True,
            file=True,
            level=logging.INFO
        )
    
    @classmethod
    def setup_database_logger(cls) -> logging.Logger:
        """
        Configura e retorna um logger específico para operações de banco de dados.
        
        Returns:
            Logger configurado para operações de banco de dados
        """
        return cls.setup(
            'FIIDatabase',
            console=True,
            file=True,
            level=logging.INFO
        )
    
    @classmethod
    def setup_file_logger(cls, file_type: str) -> logging.Logger:
        """
        Configura e retorna um logger específico para operações de arquivos.
        
        Args:
            file_type: Tipo de arquivo (ex: 'import', 'export', 'zip')
            
        Returns:
            Logger configurado para operações de arquivos
        """
        return cls.setup(
            f'fii_{file_type}',
            console=True,
            file=True,
            level=logging.INFO
        )
    
    @classmethod
    def reset_loggers(cls) -> None:
        """
        Fecha e limpa todos os loggers.
        Útil para testes e para garantir que recursos sejam liberados.
        """
        for name, logger in cls._loggers.items():
            if hasattr(logger, 'handlers'):
                for handler in logger.handlers:
                    # Certifique-se de fechar o handler para liberar recursos
                    if hasattr(handler, 'close'):
                        handler.close()
                logger.handlers.clear()
        
        # Limpa o dicionário de loggers
        cls._loggers.clear()


# Funções de conveniência para facilitar o uso

def get_logger(name: str, **kwargs) -> logging.Logger:
    """
    Função de conveniência para obter um logger configurado.
    
    Args:
        name: Nome do logger
        **kwargs: Argumentos adicionais para configuração
        
    Returns:
        Logger configurado
    """
    return LoggingManager.get_logger(name, **kwargs)


def setup_download_logger() -> logging.Logger:
    """
    Função de conveniência para configurar o logger de download.
    
    Returns:
        Logger de download configurado
    """
    return LoggingManager.setup_download_logger()


def setup_security_logger() -> logging.Logger:
    """
    Função de conveniência para configurar o logger de segurança.
    
    Returns:
        Logger de segurança configurado
    """
    return LoggingManager.setup_security_logger()


def setup_database_logger() -> logging.Logger:
    """
    Função de conveniência para configurar o logger de banco de dados.
    
    Returns:
        Logger de banco de dados configurado
    """
    return LoggingManager.setup_database_logger()


def setup_main_logger() -> logging.Logger:
    """
    Função de conveniência para configurar o logger principal do sistema.
    
    Returns:
        Logger principal configurado
    """
    return LoggingManager.get_logger('fii_main')