"""
Decoradores para operações comuns de banco de dados no Sistema de Análise de FIIs.
Centraliza padrões repetitivos e fornece utilidades para otimizar operações de BD.
"""

import functools
import time
import sqlite3

from fii_utils.logging_manager import get_logger
from fii_utils.config_manager import get_config_manager

# Obtém um logger específico para operações de banco
logger = get_logger('FIIDatabase')
config = get_config_manager()

def ensure_connection(func):
    """
    Decorator que garante que uma conexão com o banco de dados está estabelecida
    antes de executar a função decorada.
    
    O objeto deve ter um atributo 'conn' que é a conexão SQLite,
    e um método 'conectar' para estabelecer a conexão.
    
    Args:
        func: Função a ser decorada
        
    Returns:
        Wrapper que verifica e estabelece conexão quando necessário
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Verifica se a conexão existe e está aberta
        if not hasattr(self, 'conn') or self.conn is None:
            if hasattr(self, 'conectar'):
                self.conectar()
            else:
                raise AttributeError(f"Objeto {self.__class__.__name__} não possui método 'conectar'")
        
        # Executa a função original
        return func(self, *args, **kwargs)
    
    return wrapper

def transaction(func):
    """
    Decorator que executa a função em uma transação, fazendo commit em caso de
    sucesso ou rollback em caso de erro.
    
    Args:
        func: Função a ser decorada
        
    Returns:
        Wrapper que gerencia transação
    """
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Garante que uma conexão está estabelecida
        if not hasattr(self, 'conn') or self.conn is None:
            if hasattr(self, 'conectar'):
                self.conectar()
            else:
                raise AttributeError(f"Objeto {self.__class__.__name__} não possui método 'conectar'")
        
        try:
            # Executa a função
            result = func(self, *args, **kwargs)
            
            # Faz commit se a conexão estiver ativa
            if self.conn:
                self.conn.commit()
            
            return result
            
        except Exception as e:
            # Faz rollback em caso de erro
            if hasattr(self, 'conn') and self.conn:
                self.conn.rollback()
            
            # Registra o erro
            logger.error(f"Erro na transação em {func.__name__}: {e}")
            
            # Propaga a exceção
            raise
    
    return wrapper

def retry_on_db_locked(max_retries=3, delay_seconds=2):
    """
    Decorator para tentar novamente operações de banco de dados quando
    o banco está bloqueado (erro 'database is locked').
    
    Args:
        max_retries: Número máximo de tentativas
        delay_seconds: Tempo de espera entre tentativas
        
    Returns:
        Decorator configurado
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            last_error = None
            
            while attempts < max_retries:
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if "database is locked" in str(e) and attempts < max_retries - 1:
                        attempts += 1
                        logger.warning(f"Banco bloqueado. Tentativa {attempts} de {max_retries}. Aguardando {delay_seconds}s...")
                        time.sleep(delay_seconds)
                        last_error = e
                    else:
                        raise
                except Exception as e:
                    # Propaga outros erros imediatamente
                    raise
            
            # Se chegou aqui, todas as tentativas falharam
            if last_error:
                raise last_error
            else:
                raise RuntimeError(f"Todas as {max_retries} tentativas falharam sem erro específico")
        
        return wrapper
    
    return decorator

def optimize_lote_size(data_size_bytes=None):
    """
    Decorator que otimiza o tamanho do lote para operações em lote
    com base no tamanho dos dados ou configuração.
    
    Args:
        data_size_bytes: Tamanho aproximado em bytes de cada registro (opcional)
        
    Returns:
        Decorator configurado
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, registros, *args, **kwargs):
            # Obter tamanhos de lote da configuração
            lote_pequeno = config.get("db_lote_size_pequeno", 1000)
            lote_medio = config.get("db_lote_size_medio", 5000)
            lote_grande = config.get("db_lote_size_grande", 10000)
            max_lote_bytes = config.get("db_tamanho_maximo_lote_bytes", 1048576)  # 1MB
            
            # Determinar tamanho apropriado de lote
            if len(registros) <= lote_pequeno:
                # Para poucos registros, usa o lote pequeno
                tamanho_lote = lote_pequeno
            elif data_size_bytes is not None:
                # Se conhecemos o tamanho aproximado dos registros, otimizamos
                total_bytes = len(registros) * data_size_bytes
                # Calcula o tamanho do lote para não exceder max_lote_bytes
                records_per_batch = max(1, min(lote_grande, max_lote_bytes // data_size_bytes))
                tamanho_lote = records_per_batch
            elif len(registros) <= lote_medio * 10:
                # Para quantidades médias, usa o lote médio
                tamanho_lote = lote_medio
            else:
                # Para grandes volumes, usa o lote grande
                tamanho_lote = lote_grande
            
            # Registra o tamanho do lote usado
            logger.debug(f"Tamanho de lote otimizado: {tamanho_lote} para {len(registros)} registros")
            
            # Adiciona o tamanho do lote aos kwargs
            kwargs['tamanho_lote'] = tamanho_lote
            
            # Chama a função original
            return func(self, registros, *args, **kwargs)
        
        return wrapper
    
    return decorator

def log_execution_time(func):
    """
    Decorator que registra o tempo de execução de uma função.
    Útil para identificar operações de banco de dados lentas.
    
    Args:
        func: Função a ser decorada
        
    Returns:
        Wrapper que mede e registra o tempo de execução
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        
        try:
            # Executa a função
            result = func(*args, **kwargs)
            
            # Calcula o tempo
            end_time = time.time()
            execution_time = end_time - start_time
            
            # Registra o tempo
            logger.debug(f"Tempo de execução de {func.__name__}: {execution_time:.2f}s")
            
            # Se a execução foi muito lenta, registra um aviso
            if execution_time > 1.0:  # Threshold arbitrário de 1 segundo
                logger.warning(f"Operação lenta: {func.__name__} ({execution_time:.2f}s)")
            
            return result
            
        except Exception as e:
            # Em caso de erro, ainda calculamos o tempo até o erro
            end_time = time.time()
            execution_time = end_time - start_time
            
            logger.error(f"Erro em {func.__name__} após {execution_time:.2f}s: {e}")
            raise
    
    return wrapper

def prepared_statement(sql, bind_args=None):
    """
    Decorator para reutilizar prepared statements.
    
    Args:
        sql: Query SQL a ser preparada
        bind_args: Função para extrair os argumentos da bind dos args/kwargs da função
        
    Returns:
        Decorator configurado
    """
    if bind_args is None:
        # Função padrão assume que o primeiro argumento posicional é a lista de argumentos
        bind_args = lambda *args, **kwargs: args[0] if args else tuple()
    
    def decorator(func):
        # Atributos para armazenar o statement preparado
        # Serão definidos na primeira chamada
        stmt_name = f"_stmt_{func.__name__}"
        stmt_initialized = f"_stmt_{func.__name__}_initialized"
        
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            # Verifica se o statement já está preparado
            if not hasattr(self, stmt_initialized) or not getattr(self, stmt_initialized):
                # Prepara o statement
                if hasattr(self, 'cursor') and self.cursor:
                    # setattr(self, stmt_name, self.cursor.prepare(sql))
                    setattr(self, stmt_initialized, True)
                else:
                    # Se não temos cursor, apenas executamos normalmente
                    return func(self, *args, **kwargs)
            
            # Extrai os argumentos para bind
            bind_values = bind_args(*args, **kwargs)
            
            try:
                # Na verdade, SQLite não tem verdadeiro prepared statements como PostgreSQL ou MySQL
                # Então aqui apenas executamos a query com os argumentos
                self.cursor.execute(sql, bind_values)
                
                # Processa o resultado conforme a função original
                return func(self, *args, **kwargs)
                
            except Exception as e:
                logger.error(f"Erro ao executar prepared statement: {e}")
                # Em caso de erro, resetamos o flag para recriar o statement
                setattr(self, stmt_initialized, False)
                raise
        
        return wrapper
    
    return decorator