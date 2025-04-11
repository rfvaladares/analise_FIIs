import os
import sqlite3
import hashlib
import logging
from typing import Tuple, Generator
import time
from contextlib import contextmanager

def calcular_hash_arquivo(caminho_arquivo: str) -> str:
    """
    Calcula o hash MD5 de um arquivo.
    
    Args:
        caminho_arquivo: Caminho completo para o arquivo
            
    Returns:
        String com o hash MD5 hexadecimal
    """
    # Usa o algoritmo MD5 para calcular o hash
    hash_md5 = hashlib.md5()
    
    try:
        with open(caminho_arquivo, 'rb') as arquivo:
            # Lê o arquivo em blocos para não carregar tudo na memória
            for bloco in iter(lambda: arquivo.read(4096), b''):
                hash_md5.update(bloco)
                
        return hash_md5.hexdigest()
    except Exception as e:
        logger = logging.getLogger('FIIDatabase')
        logger.error(f"Erro ao calcular hash do arquivo {caminho_arquivo}: {e}")
        return ""

def otimizar_conexao_sqlite(cursor: sqlite3.Cursor) -> None:
    """
    Aplica otimizações de performance para conexão SQLite.
    
    Args:
        cursor: Cursor SQLite ativo
    """
    cursor.execute("PRAGMA synchronous = NORMAL")  # Modificado de OFF para NORMAL para mais segurança
    cursor.execute("PRAGMA journal_mode = WAL")    # Modificado de MEMORY para WAL (Write-Ahead Logging) para reduzir bloqueios
    cursor.execute("PRAGMA cache_size = 100000")  # Cerca de 100MB de cache
    cursor.execute("PRAGMA temp_store = MEMORY")
    cursor.execute("PRAGMA busy_timeout = 30000")  # 30 segundos de timeout para esperar bloqueios
    cursor.execute("PRAGMA page_size = 4096")
    
    logger = logging.getLogger('FIIDatabase')
    logger.info("Aplicadas otimizações de PRAGMA para SQLite")

@contextmanager
def conexao_banco(arquivo_db: str) -> Generator[Tuple[sqlite3.Connection, sqlite3.Cursor], None, None]:
    """
    Context manager para gerenciar conexões de banco de dados.
    Garante que a conexão seja fechada corretamente após o uso.
    
    Args:
        arquivo_db: Caminho para o arquivo de banco de dados SQLite
    
    Yields:
        Tupla (conexão, cursor) para uso no bloco with
    
    Example:
        ```python
        with conexao_banco('fundos_imobiliarios.db') as (conn, cursor):
            cursor.execute("SELECT * FROM cotacoes")
            rows = cursor.fetchall()
        # Conexão é fechada automaticamente após o bloco with
        ```
    """
    conn, cursor = conectar_banco(arquivo_db)
    try:
        yield conn, cursor
    finally:
        if conn:
            conn.close()

def conectar_banco(arquivo_db: str) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    """
    Estabelece conexão com o banco de dados SQLite.
    
    Args:
        arquivo_db: Caminho para o arquivo do banco SQLite
        
    Returns:
        Tupla (conexão, cursor)
    """
    logger = logging.getLogger('FIIDatabase')
    
    try:
        # Verificar se o banco existe
        if not os.path.exists(arquivo_db):
            logger.warning(f"Banco de dados {arquivo_db} não existe. Será criado.")
        
        # Tentativas de conexão com retry
        max_tentativas = 5
        tentativa = 0
        
        while tentativa < max_tentativas:
            try:
                conn = sqlite3.connect(arquivo_db, timeout=120.0)  # Aumentado o timeout para 120 segundos
                cursor = conn.cursor()
                otimizar_conexao_sqlite(cursor)
                
                logger.info(f"Conectado ao banco de dados {arquivo_db}")
                return conn, cursor
            except sqlite3.OperationalError as e:
                if "database is locked" in str(e) and tentativa < max_tentativas - 1:
                    tentativa += 1
                    logger.warning(f"Banco de dados bloqueado. Tentativa {tentativa} de {max_tentativas}. Aguardando 2 segundos...")
                    time.sleep(2)  # Aguarda 2 segundos antes de tentar novamente
                else:
                    raise
        
        # Se chegou aqui é porque esgotou as tentativas
        raise sqlite3.OperationalError(f"Não foi possível conectar ao banco de dados após {max_tentativas} tentativas.")
    except sqlite3.Error as e:
        logger.error(f"Erro ao conectar ao banco de dados: {e}")
        if 'conn' in locals() and conn:
            conn.close()
        raise