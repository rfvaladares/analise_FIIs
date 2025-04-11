import os
import sqlite3
import concurrent.futures
import traceback
from typing import List, Dict, Tuple, Optional

# Importações adicionais
from fii_utils.cache_manager import get_cache_manager, cached, CachePolicy
from fii_utils.db_decorators import (
    ensure_connection, transaction, retry_on_db_locked, optimize_lote_size,
    log_execution_time
)

from fii_utils.parsers import processar_chunk, CotacaoParser, ArquivoCotacao
from fii_utils.db_utils import conectar_banco
from fii_utils.logging_manager import get_logger

# Importação no nível do módulo para evitar importação circular dentro do método
# Isso é importante para manter a clareza do código
from db_managers.arquivos import ArquivosProcessadosManager

class CotacoesManager:
    """
    Gerencia a tabela de cotações no banco de dados.
    Responsável por inserir, atualizar e consultar cotações dos FIIs.
    """
    
    def __init__(self, arquivo_db: str = 'fundos_imobiliarios.db', num_workers: int = None):
        self.arquivo_db = arquivo_db
        self.conn = None
        self.cursor = None
        self.logger = get_logger('FIIDatabase')
        self.parser = CotacaoParser()
        self.num_workers = num_workers or os.cpu_count() // 2  # Por padrão, usa metade dos cores
        
        # Inicializar sistema de cache
        self.cache_manager = get_cache_manager()
        
        # Registrar políticas de cache específicas para esta classe
        self.cache_manager.register_policy('cotacoes_lista', CachePolicy(ttl=3600, max_size=100))  # 1 hora
        self.cache_manager.register_policy('cotacoes_ultima_data', CachePolicy(ttl=600, max_size=10))  # 10 minutos
        self.cache_manager.register_policy('cotacoes_estatisticas', CachePolicy(ttl=1800, max_size=10))  # 30 minutos
    
    def conectar(self) -> None:
        """
        Conecta ao banco de dados existente.
        """
        self.conn, self.cursor = conectar_banco(self.arquivo_db)
    
    def criar_tabela(self) -> None:
        """
        Cria a tabela de cotações se não existir.
        """
        if not self.conn:
            self.conectar()
            
        try:
            # Cria tabela de cotações (sem a coluna 'media')
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS cotacoes (
                data TEXT,
                codigo TEXT,
                abertura REAL,
                maxima REAL,
                minima REAL,
                fechamento REAL,
                volume REAL,
                negocios INTEGER,
                quantidade INTEGER,
                PRIMARY KEY (data, codigo)
            )
            ''')
            
            # Cria índices para otimizar consultas
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_cotacoes_data ON cotacoes(data)')
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_cotacoes_codigo ON cotacoes(codigo)')
            
            self.conn.commit()
            self.logger.info("Tabela cotacoes criada/verificada com sucesso")
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao criar tabela de cotações: {e}")
            self.conn.rollback()
            raise
    
    @ensure_connection
    @transaction
    def limpar_periodo(self, data_inicio: str, data_fim: str) -> int:
        """
        Remove registros de cotações em um determinado período.
        
        Args:
            data_inicio: Data inicial no formato YYYY-MM-DD
            data_fim: Data final no formato YYYY-MM-DD
            
        Returns:
            Número de registros removidos
        """
        try:
            self.cursor.execute('''
            DELETE FROM cotacoes 
            WHERE data BETWEEN ? AND ?
            ''', (data_inicio, data_fim))
            
            registros_removidos = self.cursor.rowcount
            self.logger.info(f"Removidos {registros_removidos} registros para o período {data_inicio} a {data_fim}")
            
            # Invalidar caches relacionados
            cache = get_cache_manager()
            cache.invalidate('cotacoes_estatisticas')
            cache.invalidate('cotacoes_ultima_data')
            
            return registros_removidos
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao limpar período de cotações: {e}")
            return 0
    
    @ensure_connection
    @retry_on_db_locked()
    @optimize_lote_size(data_size_bytes=100)  # Estimativa de tamanho por registro
    def inserir_cotacoes(self, registros: List[Tuple], tamanho_lote: int = 5000) -> int:
        """
        Insere múltiplos registros de cotações no banco com tratamento de conflitos.
        
        Args:
            registros: Lista de tuplas com os dados dos registros
            tamanho_lote: Tamanho do lote para inserções em batch (calculado pelo decorator optimize_lote_size)
            
        Returns:
            Número de registros inseridos
        """
        registros_inseridos = 0
        
        try:
            inserir_query = '''
            INSERT OR IGNORE INTO cotacoes 
            (data, codigo, abertura, maxima, minima, fechamento, volume, negocios, quantidade)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            '''
            
            # Insere em lotes usando o tamanho otimizado pelo decorator
            for i in range(0, len(registros), tamanho_lote):
                lote = registros[i:i+tamanho_lote]
                
                self.cursor.executemany(inserir_query, lote)
                registros_inseridos += len(lote)
                self.conn.commit()  # Commit após cada lote
                
                if i % 20000 == 0 and i > 0:
                    self.logger.info(f"Progresso: {i}/{len(registros)} registros inseridos")
            
            self.logger.info(f"Total de {registros_inseridos} registros inseridos com sucesso")
            
            # Invalidar caches relacionados
            cache = get_cache_manager()
            cache.invalidate('cotacoes_ultima_data')
            cache.invalidate('cotacoes_estatisticas')
            
            return registros_inseridos
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao inserir cotações: {e}")
            self.conn.rollback()
            return 0
    
    def _registrar_arquivo_processado(self, arquivo_cotacao: ArquivoCotacao, 
                                      registros_inseridos: int,
                                      remover_txt: bool = True) -> None:
        """
        Método auxiliar para registrar um arquivo como processado.
        Extrai essa funcionalidade para simplificar o método processar_arquivo.
        
        Args:
            arquivo_cotacao: Objeto ArquivoCotacao a ser registrado
            registros_inseridos: Número de registros inseridos
            remover_txt: Se deve remover o arquivo TXT após processamento
        """
        try:
            # Instancia o gerenciador de arquivos processados
            arquivos_manager = ArquivosProcessadosManager(self.arquivo_db)
            arquivos_manager.conectar()
            arquivos_manager.registrar_arquivo_processado(
                arquivo_cotacao, 
                registros_inseridos,
                remover_txt=remover_txt
            )
            arquivos_manager.fechar_conexao()
        except Exception as e:
            self.logger.error(f"Erro ao registrar arquivo processado: {e}")
    
    @log_execution_time
    def processar_arquivo(self, arquivo_cotacao: ArquivoCotacao, 
                           substituir_existentes: bool = False,
                           remover_txt: bool = True) -> int:
        """
        Processa um arquivo de cotações e insere os registros no banco.
        
        Args:
            arquivo_cotacao: Objeto ArquivoCotacao a ser processado
            substituir_existentes: Se True, remove registros existentes do período
            remover_txt: Se deve remover o arquivo TXT após processamento
            
        Returns:
            Número de registros inseridos
        """
        if not self.conn:
            self.conectar()
            
        self.logger.info(f"Processando arquivo: {arquivo_cotacao}")
        registros_inseridos = 0
        
        try:
            # Se for para substituir, remove registros existentes no período
            if substituir_existentes:
                data_inicio = arquivo_cotacao.data_inicio.strftime('%Y-%m-%d')
                data_fim = arquivo_cotacao.data_fim.strftime('%Y-%m-%d')
                self.limpar_periodo(data_inicio, data_fim)
                
            # Verifica o tipo do arquivo para escolher a estratégia de processamento
            if arquivo_cotacao.tipo in ['anual', 'mensal']:
                # Arquivos grandes são processados em chunks
                registros_inseridos = self._processar_arquivo_chunks(arquivo_cotacao)
            else:
                # Arquivos diários são processados diretamente
                registros_inseridos = self._processar_arquivo_direto(arquivo_cotacao)
                
            # Registra o arquivo como processado (agora com opção de remover TXT)
            if registros_inseridos > 0:
                self._registrar_arquivo_processado(
                    arquivo_cotacao, 
                    registros_inseridos,
                    remover_txt=remover_txt
                )
                
            return registros_inseridos
            
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo {arquivo_cotacao.caminho}: {e}")
            self.logger.error(traceback.format_exc())
            return 0
    
    @log_execution_time
    def _processar_arquivo_chunks(self, arquivo_cotacao: ArquivoCotacao) -> int:
        """
        Processa um arquivo grande dividindo-o em chunks para processamento paralelo.
        
        Args:
            arquivo_cotacao: Objeto ArquivoCotacao com informações do arquivo
            
        Returns:
            Número de registros inseridos
        """
        self.logger.info(f"Processando arquivo em chunks: {arquivo_cotacao}")
        registros_inseridos = 0
        
        try:
            # Lê o arquivo e divide em chunks
            chunks = []
            current_chunk = []
            chunk_size = 100000  # Tamanho do chunk
            
            # Lê o arquivo uma vez para dividir em chunks
            with open(arquivo_cotacao.caminho, 'r', encoding='iso-8859-1') as arquivo:
                self.logger.info(f"Dividindo arquivo {arquivo_cotacao.nome_arquivo} em chunks...")
                for i, linha in enumerate(arquivo):
                    # Verifica se é registro tipo 01 (cotações) e com BDI 12 (FII)
                    if len(linha) >= 245 and linha[0:2] == '01' and linha[10:12].strip() == '12':
                        current_chunk.append(linha)
                    
                    if i % chunk_size == chunk_size - 1:
                        if current_chunk:  # Só adiciona se houver registros de FII
                            chunks.append((current_chunk.copy(), self.parser))
                            current_chunk = []
                            
                            # Log de progresso na leitura do arquivo
                            if len(chunks) % 10 == 0:
                                self.logger.info(f"Criados {len(chunks)} chunks até agora...")
            
            # Adiciona o último chunk se houver
            if current_chunk:
                chunks.append((current_chunk, self.parser))
            
            total_chunks = len(chunks)
            self.logger.info(f"Arquivo {arquivo_cotacao.nome_arquivo} dividido em {total_chunks} chunks de FIIs")
            
            # Ajusta o número de workers com base na quantidade de chunks
            num_workers = min(self.num_workers, total_chunks)
            
            if num_workers < 1:
                num_workers = 1  # Garantir pelo menos 1 worker
                
            self.logger.info(f"Iniciando processamento paralelo com {num_workers} workers")
            
            # Fecha a conexão com o banco antes de iniciar o processamento paralelo
            # para evitar que ela seja compartilhada entre processos
            if self.conn:
                self.cursor = None
                self.conn.close()
                self.conn = None
            
            # Processa os chunks em paralelo
            todos_registros = []
            chunks_processados = 0
            chunks_com_erro = 0
            
            with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
                # Submete todos os chunks para processamento
                future_to_chunk = {executor.submit(processar_chunk, chunk): i for i, chunk in enumerate(chunks)}
                
                # Coleta os resultados à medida que ficam prontos
                for future in concurrent.futures.as_completed(future_to_chunk):
                    chunk_index = future_to_chunk[future]
                    try:
                        registros_chunk = future.result()
                        if registros_chunk:
                            todos_registros.extend(registros_chunk)
                            chunks_processados += 1
                        else:
                            self.logger.warning(f"Chunk {chunk_index} retornou vazio (possível erro)")
                            chunks_com_erro += 1
                    except Exception as e:
                        self.logger.error(f"Erro ao processar chunk {chunk_index}: {e}")
                        chunks_com_erro += 1
                        
                    # Log de progresso
                    progresso_total = chunks_processados + chunks_com_erro
                    if progresso_total % 10 == 0 or progresso_total == total_chunks:
                        self.logger.info(f"Progresso: {progresso_total}/{total_chunks} chunks processados")
            
            # Reconecta ao banco para inserir os registros
            self.conectar()
            
            # Status do processamento paralelo
            self.logger.info(f"Processamento paralelo concluído: {chunks_processados} chunks processados, {chunks_com_erro} chunks com erro")
            self.logger.info(f"Total de registros coletados: {len(todos_registros)}")
            
            # Insere os registros coletados no banco
            if todos_registros:
                registros_inseridos = self.inserir_cotacoes(todos_registros)
                
            # Log final
            self.logger.info(f"Arquivo {arquivo_cotacao.nome_arquivo} processado em chunks. Registros inseridos: {registros_inseridos}")
            return registros_inseridos
            
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo {arquivo_cotacao.caminho} em chunks: {e}")
            self.logger.error(traceback.format_exc())
            
            # Reconecta ao banco se a conexão foi fechada
            if not self.conn:
                try:
                    self.conectar()
                except Exception as conn_err:
                    self.logger.error(f"Erro ao reconectar ao banco: {conn_err}")
                    
            return 0
    
    def _processar_arquivo_direto(self, arquivo_cotacao: ArquivoCotacao) -> int:
        """
        Processa um arquivo pequeno diretamente, sem divisão em chunks.
        
        Args:
            arquivo_cotacao: Objeto ArquivoCotacao com informações do arquivo
            
        Returns:
            Número de registros inseridos
        """
        self.logger.info(f"Processando arquivo diretamente: {arquivo_cotacao}")
        registros = []
        
        try:
            with open(arquivo_cotacao.caminho, 'r', encoding='iso-8859-1') as arquivo:
                for linha in arquivo:
                    # Verificar se é um registro de FII (tipo 01 e BDI 12)
                    if len(linha) >= 245 and linha[0:2] == '01' and linha[10:12].strip() == '12':
                        registro = self.parser.parse_linha(linha)
                        if registro:
                            registros.append((
                                registro['data'],
                                registro['codigo'],
                                registro['abertura'],
                                registro['maxima'],
                                registro['minima'],
                                registro['fechamento'],
                                registro['volume'],
                                registro['negocios'],
                                registro['quantidade']
                            ))
            
            # Insere os registros no banco
            registros_inseridos = 0
            if registros:
                registros_inseridos = self.inserir_cotacoes(registros)
                
            self.logger.info(f"Arquivo {arquivo_cotacao.nome_arquivo} processado diretamente. Registros inseridos: {registros_inseridos}")
            return registros_inseridos
            
        except Exception as e:
            self.logger.error(f"Erro ao processar arquivo {arquivo_cotacao.caminho} diretamente: {e}")
            self.logger.error(traceback.format_exc())
            return 0
    
    @ensure_connection
    @cached('cotacoes_ultima_data', key_func=lambda self: 'ultima_data')
    def obter_ultima_data(self) -> Optional[str]:
        """
        Retorna a data da última cotação no banco de dados.
        
        Returns:
            Data no formato YYYY-MM-DD ou None se não houver registros
        """
        try:
            self.cursor.execute("SELECT MAX(data) FROM cotacoes")
            return self.cursor.fetchone()[0]
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao obter última data: {e}")
            return None
    
    @ensure_connection
    @cached('cotacoes_estatisticas', key_func=lambda self: 'estatisticas_gerais')
    def obter_estatisticas(self) -> Dict:
        """
        Obtém estatísticas sobre os dados de cotações.
        
        Returns:
            Dicionário com estatísticas
        """
        try:
            # Contagem de registros na tabela cotacoes
            self.cursor.execute("SELECT COUNT(*) FROM cotacoes")
            total_registros = self.cursor.fetchone()[0]
            
            # Contagem de FIIs únicos
            self.cursor.execute("SELECT COUNT(DISTINCT codigo) FROM cotacoes")
            total_fiis = self.cursor.fetchone()[0]
            
            # Intervalo de datas
            self.cursor.execute("SELECT MIN(data), MAX(data) FROM cotacoes")
            data_min, data_max = self.cursor.fetchone()
            
            return {
                'total_registros': total_registros,
                'total_fiis': total_fiis,
                'data_minima': data_min,
                'data_maxima': data_max
            }
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao obter estatísticas: {e}")
            return {
                'total_registros': 0,
                'total_fiis': 0,
                'data_minima': None,
                'data_maxima': None
            }
    
    @ensure_connection
    @cached('cotacoes_lista', key_func=lambda self: 'listar_fiis')
    def listar_fiis(self) -> List[str]:
        """
        Lista todos os códigos de FIIs presentes no banco.
        
        Returns:
            Lista de códigos de FIIs
        """
        try:
            self.cursor.execute("SELECT DISTINCT codigo FROM cotacoes ORDER BY codigo")
            return [row[0] for row in self.cursor.fetchall()]
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao listar FIIs: {e}")
            return []
    
    def fechar_conexao(self) -> None:
        """
        Fecha a conexão com o banco de dados.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            self.logger.info("Conexão com o banco de dados fechada")