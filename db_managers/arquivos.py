import os
import sqlite3
from datetime import datetime
from typing import Dict, List, Tuple, Set

# Importações adicionais para otimização
from fii_utils.cache_manager import get_cache_manager, cached, CachePolicy
from fii_utils.db_decorators import ensure_connection, transaction

from fii_utils.db_utils import calcular_hash_arquivo, conectar_banco
from fii_utils.parsers import ArquivoCotacao
from fii_utils.logging_manager import get_logger
from fii_utils.zip_utils import normalizar_nome_arquivo

class ArquivosProcessadosManager:
    """
    Gerencia a tabela de arquivos processados no banco de dados.
    Rastreia quais arquivos foram incorporados ao banco e seus hashes.
    
    O sistema utiliza exclusivamente arquivos ZIP como referência para status de processamento.
    Arquivos TXT são extraídos temporariamente e removidos após processamento.
    """
    
    def __init__(self, arquivo_db: str = 'fundos_imobiliarios.db'):
        self.arquivo_db = arquivo_db
        self.conn = None
        self.cursor = None
        self.logger = get_logger('FIIDatabase')
        self.arquivos_processados = {}  # {nome_arquivo: hash_md5}
        
        # Inicializar sistema de cache
        self.cache_manager = get_cache_manager()
        
        # Registrar políticas de cache específicas
        self.cache_manager.register_policy('arquivos_processados', CachePolicy(ttl=1800, max_size=200))  # 30 minutos
    
    def conectar(self) -> None:
        """
        Conecta ao banco de dados existente e carrega o registro de arquivos processados.
        """
        try:
            # Conecta ao banco
            self.conn, self.cursor = conectar_banco(self.arquivo_db)
            
            # Carrega os hashes dos arquivos já processados (apenas ZIPs)
            self.cursor.execute("SELECT nome_arquivo, hash_md5 FROM arquivos_processados")
            rows = self.cursor.fetchall()
            
            for nome, hash_md5 in rows:
                if hash_md5:  # Ignora registros com hash NULL (não deveria acontecer mais)
                    self.arquivos_processados[nome] = hash_md5
            
            self.logger.info(f"Conectado ao banco de dados {self.arquivo_db}")
            self.logger.info(f"Encontrados {len(self.arquivos_processados)} arquivos ZIP com hash registrado")
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao conectar ao banco de dados: {e}")
            if self.conn:
                self.conn.close()
            raise
    
    @ensure_connection
    @transaction
    def criar_tabela(self) -> None:
        """
        Cria a tabela de arquivos processados se não existir.
        """
        try:
            # Cria tabela para controle de arquivos processados
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS arquivos_processados (
                nome_arquivo TEXT PRIMARY KEY,
                tipo TEXT,
                data_processamento TEXT,
                registros_adicionados INTEGER,
                hash_md5 TEXT
            )
            ''')
            
            self.logger.info("Tabela arquivos_processados criada/verificada com sucesso")
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao criar tabela de arquivos processados: {e}")
            raise
    
    def obter_caminho_zip(self, caminho_txt: str) -> Tuple[str, str, bool]:
        """
        A partir de um caminho de arquivo TXT, retorna o caminho do ZIP correspondente.
        
        Args:
            caminho_txt: Caminho do arquivo TXT
            
        Returns:
            Tupla (caminho_zip, nome_zip, zip_existe)
        """
        # Normaliza o caminho para garantir a extensão correta
        nome_txt = os.path.basename(caminho_txt)
        nome_base, extensao = normalizar_nome_arquivo(nome_txt)
        
        # Garante que estamos trabalhando com TXT
        if extensao != '.TXT':
            self.logger.warning(f"O arquivo {caminho_txt} não é um arquivo TXT")
            return caminho_txt, nome_txt, False
        
        # Determina o caminho do arquivo ZIP correspondente
        diretorio = os.path.dirname(caminho_txt)
        nome_zip = nome_base + '.ZIP'
        caminho_zip = os.path.join(diretorio, nome_zip)
        
        # Verifica se o arquivo ZIP existe
        zip_existe = os.path.exists(caminho_zip)
        
        if not zip_existe:
            self.logger.warning(f"Arquivo ZIP {caminho_zip} não encontrado para o TXT {nome_txt}")
            
        return caminho_zip, nome_zip, zip_existe
    
    @ensure_connection
    @transaction
    def registrar_arquivo_processado(self, arquivo_cotacao: ArquivoCotacao, 
                                     registros_adicionados: int,
                                     remover_txt: bool = True) -> None:
        """
        Registra um arquivo como processado com seu hash MD5.
        Sempre registra o arquivo ZIP, não o TXT, para economizar espaço.
        
        Args:
            arquivo_cotacao: Objeto ArquivoCotacao com informações do arquivo
            registros_adicionados: Número de registros inseridos
            remover_txt: Se deve remover o arquivo TXT após o processamento
        """
        try:
            # Obtém o nome do arquivo TXT (o que foi processado)
            caminho_txt = arquivo_cotacao.caminho
            nome_txt = arquivo_cotacao.nome_arquivo
            
            # Obtém informações do arquivo ZIP correspondente
            caminho_zip, nome_zip, zip_existe = self.obter_caminho_zip(caminho_txt)
            
            if zip_existe:
                # Se o ZIP existe, registramos ele e calculamos seu hash
                nome_arquivo_registrar = nome_zip
                caminho_hash = caminho_zip
                pode_remover_txt = True
            else:
                # Caso excepcional: se o ZIP não existe, usamos o TXT
                self.logger.warning(f"Usando TXT para registro pois o ZIP não existe: {nome_txt}")
                nome_arquivo_registrar = nome_txt
                caminho_hash = caminho_txt
                pode_remover_txt = False  # Não podemos remover o TXT se não temos o ZIP
            
            # Calcula o hash do arquivo apropriado (ZIP ou TXT)
            hash_md5 = calcular_hash_arquivo(caminho_hash)
            
            # Registra o arquivo como processado
            self.cursor.execute('''
            INSERT OR REPLACE INTO arquivos_processados 
            (nome_arquivo, tipo, data_processamento, registros_adicionados, hash_md5)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                nome_arquivo_registrar, 
                arquivo_cotacao.tipo,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                registros_adicionados,
                hash_md5
            ))
            
            # Atualiza o dicionário em memória
            self.arquivos_processados[nome_arquivo_registrar] = hash_md5
            
            # Invalidar cache de arquivos processados
            self.cache_manager.invalidate('arquivos_processados')
            
            self.logger.info(f"Arquivo {nome_arquivo_registrar} registrado como processado")
            
            # Se solicitado e possível, remover o arquivo TXT para economizar espaço
            if remover_txt and pode_remover_txt and os.path.exists(caminho_txt):
                try:
                    os.remove(caminho_txt)
                    self.logger.info(f"Arquivo TXT {caminho_txt} removido para economizar espaço")
                except Exception as e:
                    self.logger.warning(f"Não foi possível remover o arquivo TXT {caminho_txt}: {e}")
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao registrar arquivo processado: {e}")
            raise
    
    @ensure_connection
    @cached('arquivos_processados', key_func=lambda self, caminho_arquivo: f'verificacao:{os.path.basename(caminho_arquivo)}')
    def verificar_arquivo_processado(self, caminho_arquivo: str) -> Tuple[bool, bool]:
        """
        Verifica se um arquivo já foi processado e se foi modificado.
        Considera apenas arquivos ZIP - os TXT são tratados como temporários.
        
        Args:
            caminho_arquivo: Caminho completo do arquivo (TXT ou ZIP)
            
        Returns:
            Tupla (foi_processado, foi_modificado)
        """
        # Determina o caminho e nome do arquivo ZIP para verificação
        nome_arquivo = os.path.basename(caminho_arquivo)
        nome_base, extensao = normalizar_nome_arquivo(nome_arquivo)
        
        # Se o arquivo fornecido é um TXT, encontra o ZIP correspondente
        if extensao == '.TXT':
            diretorio = os.path.dirname(caminho_arquivo)
            nome_arquivo = nome_base + '.ZIP' 
            caminho_arquivo = os.path.join(diretorio, nome_arquivo)
            
            # Se o ZIP não existe, consideramos o arquivo como não processado
            if not os.path.exists(caminho_arquivo):
                self.logger.info(f"Arquivo ZIP {nome_arquivo} não existe, considerando não processado")
                return False, False
                
        # Se o arquivo fornecido não é um ZIP, retorna não processado
        elif extensao != '.ZIP':
            self.logger.warning(f"Arquivo {nome_arquivo} não é nem ZIP nem TXT")
            return False, False
        
        # Verifica se o arquivo ZIP está registrado
        is_registered = nome_arquivo in self.arquivos_processados
        
        if not is_registered:
            self.logger.info(f"Arquivo ZIP {nome_arquivo} não encontrado no registro")
            return False, False
        
        # Calcula o hash atual do arquivo ZIP
        hash_atual = calcular_hash_arquivo(caminho_arquivo)
        hash_anterior = self.arquivos_processados[nome_arquivo]
        
        # Compara os hashes
        foi_modificado = hash_atual != hash_anterior
        
        if foi_modificado:
            self.logger.info(f"Arquivo ZIP {nome_arquivo} foi modificado (hash diferente)")
        else:
            self.logger.info(f"Arquivo ZIP {nome_arquivo} não mudou desde o último processamento (mesmo hash)")
        
        return True, foi_modificado
    
    @ensure_connection
    @cached('arquivos_processados', key_func=lambda self: 'listar_todos')
    def listar_arquivos_processados(self) -> List[Dict]:
        """
        Lista todos os arquivos processados e suas informações.
        
        Returns:
            Lista de dicionários com informações dos arquivos
        """
        try:
            self.cursor.execute('''
            SELECT nome_arquivo, tipo, data_processamento, registros_adicionados, hash_md5
            FROM arquivos_processados
            ORDER BY tipo, nome_arquivo
            ''')
            
            rows = self.cursor.fetchall()
            result = []
            
            for row in rows:
                result.append({
                    'nome_arquivo': row[0],
                    'tipo': row[1],
                    'data_processamento': row[2],
                    'registros_adicionados': row[3],
                    'hash_md5': row[4]
                })
            
            return result
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao listar arquivos processados: {e}")
            return []
    
    @ensure_connection
    @cached('arquivos_processados', key_func=lambda self, diretorio: f'pendentes:{diretorio}')
    def verificar_arquivos_zip_pendentes(self, diretorio: str) -> Set[str]:
        """
        Verifica se há arquivos ZIP no diretório que ainda não foram processados.
        
        Args:
            diretorio: Diretório onde buscar os arquivos
            
        Returns:
            Conjunto de caminhos de arquivos ZIP pendentes
        """
        # Obter a lista de arquivos ZIP já processados
        zips_processados = {nome for nome in self.arquivos_processados if nome.upper().endswith('.ZIP')}
        
        # Obter a lista de arquivos ZIP no diretório
        zips_pendentes = set()
        
        for nome_arquivo in os.listdir(diretorio):
            nome_upper = nome_arquivo.upper()
            if nome_upper.startswith('COTAHIST_') and nome_upper.endswith('.ZIP'):
                # Verifica se o arquivo já foi processado
                if nome_upper not in zips_processados:
                    zips_pendentes.add(os.path.join(diretorio, nome_arquivo))
        
        return zips_pendentes
    
    def fechar_conexao(self) -> None:
        """
        Fecha a conexão com o banco de dados.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            self.logger.info("Conexão com o banco de dados fechada")