import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

# Importações adicionais para otimização
from fii_utils.cache_manager import get_cache_manager, cached, CachePolicy
from fii_utils.db_decorators import ensure_connection, transaction, retry_on_db_locked

from fii_utils.db_utils import conectar_banco
from fii_utils.logging_manager import get_logger

class EventosCorporativosManager:
    """
    Classe responsável por gerenciar a tabela de eventos corporativos no banco de dados.
    Permite criar a tabela, inserir novos eventos e consultar eventos existentes.
    """
    
    def __init__(self, arquivo_db: str = 'fundos_imobiliarios.db'):
        self.arquivo_db = arquivo_db
        self.conn = None
        self.cursor = None
        self.logger = get_logger('FIIDatabase')
        
        # Inicializar sistema de cache
        self.cache_manager = get_cache_manager()
        
        # Registrar políticas de cache específicas para eventos corporativos
        self.cache_manager.register_policy('eventos_corporativos', CachePolicy(ttl=3600, max_size=100))  # 1 hora
    
    def conectar(self) -> None:
        """
        Conecta ao banco de dados existente.
        """
        self.conn, self.cursor = conectar_banco(self.arquivo_db)
    
    @ensure_connection
    @transaction
    def criar_tabela(self) -> None:
        """
        Cria a tabela de eventos corporativos se ela não existir.
        """
        try:
            self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS eventos_corporativos (
                codigo TEXT,
                data TEXT,
                tipo_evento TEXT CHECK(tipo_evento IN ('grupamento', 'desdobramento')),
                fator REAL CHECK(fator > 0),
                data_registro TEXT,
                PRIMARY KEY (codigo, data, tipo_evento)
            )
            ''')
            
            # Índice para otimizar consultas
            self.cursor.execute('CREATE INDEX IF NOT EXISTS idx_eventos_codigo ON eventos_corporativos(codigo)')
            
            self.logger.info("Tabela eventos_corporativos criada/verificada com sucesso")
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao criar tabela de eventos corporativos: {e}")
            raise
    
    def _validar_evento(self, evento: Dict) -> None:
        """
        Valida os campos de um evento antes da inserção.
        
        Args:
            evento: Dicionário com os dados do evento
            
        Raises:
            ValueError: Se algum campo for inválido
        """
        # Verificar campos obrigatórios
        campos_obrigatorios = ['codigo', 'data', 'evento', 'fator']
        for campo in campos_obrigatorios:
            if campo not in evento:
                raise ValueError(f"Campo obrigatório ausente: {campo}")
        
        # Validar tipo de evento
        if evento['evento'] not in ['grupamento', 'desdobramento']:
            raise ValueError(f"Tipo de evento inválido: {evento['evento']}. Use 'grupamento' ou 'desdobramento'")
        
        # Validar formato da data
        try:
            datetime.strptime(evento['data'], '%Y-%m-%d')
        except ValueError:
            raise ValueError(f"Formato de data inválido: {evento['data']}. Use o formato YYYY-MM-DD")
        
        # Validar fator
        if not isinstance(evento['fator'], (int, float)) or evento['fator'] <= 0:
            raise ValueError(f"Fator inválido: {evento['fator']}. Deve ser um número positivo")
    
    @ensure_connection
    @transaction
    def inserir_evento(self, evento: Dict) -> bool:
        """
        Insere um único evento na tabela.
        
        Args:
            evento: Dicionário com os dados do evento
            
        Returns:
            True se inserido com sucesso, False caso contrário
        """
        try:
            self._validar_evento(evento)
            
            self.cursor.execute('''
            INSERT OR REPLACE INTO eventos_corporativos 
            (codigo, data, tipo_evento, fator, data_registro)
            VALUES (?, ?, ?, ?, ?)
            ''', (
                evento['codigo'],
                evento['data'],
                evento['evento'],
                evento['fator'],
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))
            
            self.logger.info(f"Evento inserido: {evento['codigo']} - {evento['evento']} em {evento['data']}")
            
            # Invalidar cache
            self.cache_manager.invalidate('eventos_corporativos', f'listar:{evento["codigo"]}')
            self.cache_manager.invalidate('eventos_corporativos', 'listar:todos')
            
            return True
            
        except (sqlite3.Error, ValueError) as e:
            self.logger.error(f"Erro ao inserir evento {evento}: {e}")
            return False
    
    @ensure_connection
    @retry_on_db_locked()
    def inserir_eventos(self, lista_eventos: List[Dict]) -> int:
        """
        Insere múltiplos eventos na tabela.
        
        Args:
            lista_eventos: Lista de dicionários com dados dos eventos
            
        Returns:
            Número de eventos inseridos com sucesso
        """
        eventos_inseridos = 0
        
        try:
            for evento in lista_eventos:
                if self.inserir_evento(evento):
                    eventos_inseridos += 1
                    
            self.logger.info(f"Inseridos {eventos_inseridos} de {len(lista_eventos)} eventos")
            
            # Invalidar cache completamente após inserção em massa
            self.cache_manager.invalidate('eventos_corporativos')
            
            return eventos_inseridos
            
        except Exception as e:
            self.logger.error(f"Erro ao inserir eventos em lote: {e}")
            return eventos_inseridos
    
    @ensure_connection
    @cached('eventos_corporativos', key_func=lambda self, codigo=None: f'listar:{codigo if codigo else "todos"}')
    def listar_eventos(self, codigo: Optional[str] = None) -> List[Dict]:
        """
        Lista eventos corporativos, opcionalmente filtrados por código.
        
        Args:
            codigo: Código do FII para filtrar (opcional)
            
        Returns:
            Lista de dicionários com os eventos
        """
        try:
            if codigo:
                self.cursor.execute('''
                SELECT codigo, data, tipo_evento, fator, data_registro
                FROM eventos_corporativos
                WHERE codigo = ?
                ORDER BY data
                ''', (codigo,))
            else:
                self.cursor.execute('''
                SELECT codigo, data, tipo_evento, fator, data_registro
                FROM eventos_corporativos
                ORDER BY codigo, data
                ''')
                
            rows = self.cursor.fetchall()
            result = []
            
            for row in rows:
                result.append({
                    'codigo': row[0],
                    'data': row[1],
                    'evento': row[2],
                    'fator': row[3],
                    'data_registro': row[4]
                })
                
            return result
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao listar eventos: {e}")
            return []
    
    @ensure_connection
    @transaction
    def remover_evento(self, codigo: str, data: str, tipo_evento: str) -> bool:
        """
        Remove um evento corporativo específico.
        
        Args:
            codigo: Código do FII
            data: Data do evento (YYYY-MM-DD)
            tipo_evento: Tipo do evento ('grupamento' ou 'desdobramento')
            
        Returns:
            True se removido com sucesso, False caso contrário
        """
        try:
            self.cursor.execute('''
            DELETE FROM eventos_corporativos
            WHERE codigo = ? AND data = ? AND tipo_evento = ?
            ''', (codigo, data, tipo_evento))
            
            rows_affected = self.cursor.rowcount
            
            if rows_affected > 0:
                self.logger.info(f"Evento removido: {codigo} - {tipo_evento} em {data}")
                
                # Invalidar cache
                self.cache_manager.invalidate('eventos_corporativos', f'listar:{codigo}')
                self.cache_manager.invalidate('eventos_corporativos', 'listar:todos')
                
                return True
            else:
                self.logger.warning(f"Evento não encontrado para remoção: {codigo} - {tipo_evento} em {data}")
                return False
                
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao remover evento: {e}")
            return False
    
    @ensure_connection
    @cached('eventos_corporativos', key_func=lambda self, data_inicio, data_fim: f'periodo:{data_inicio}-{data_fim}')
    def obter_eventos_por_periodo(self, data_inicio: str, data_fim: str) -> List[Dict]:
        """
        Obtém eventos corporativos em um período específico.
        
        Args:
            data_inicio: Data inicial (YYYY-MM-DD)
            data_fim: Data final (YYYY-MM-DD)
            
        Returns:
            Lista de dicionários com os eventos no período
        """
        try:
            self.cursor.execute('''
            SELECT codigo, data, tipo_evento, fator, data_registro
            FROM eventos_corporativos
            WHERE data BETWEEN ? AND ?
            ORDER BY data, codigo
            ''', (data_inicio, data_fim))
            
            rows = self.cursor.fetchall()
            result = []
            
            for row in rows:
                result.append({
                    'codigo': row[0],
                    'data': row[1],
                    'evento': row[2],
                    'fator': row[3],
                    'data_registro': row[4]
                })
                
            return result
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao obter eventos por período: {e}")
            return []
    
    @ensure_connection
    @transaction
    def atualizar_fator(self, codigo: str, data: str, tipo_evento: str, novo_fator: float) -> bool:
        """
        Atualiza o fator de um evento existente.
        
        Args:
            codigo: Código do FII
            data: Data do evento (YYYY-MM-DD)
            tipo_evento: Tipo do evento ('grupamento' ou 'desdobramento')
            novo_fator: Novo valor do fator
            
        Returns:
            True se atualizado com sucesso, False caso contrário
        """
        try:
            if novo_fator <= 0:
                raise ValueError(f"Fator inválido: {novo_fator}. Deve ser um número positivo")
                
            self.cursor.execute('''
            UPDATE eventos_corporativos
            SET fator = ?, data_registro = ?
            WHERE codigo = ? AND data = ? AND tipo_evento = ?
            ''', (
                novo_fator,
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                codigo,
                data,
                tipo_evento
            ))
            
            rows_affected = self.cursor.rowcount
            
            if rows_affected > 0:
                self.logger.info(f"Fator atualizado para evento: {codigo} - {tipo_evento} em {data}")
                
                # Invalidar cache
                self.cache_manager.invalidate('eventos_corporativos', f'listar:{codigo}')
                self.cache_manager.invalidate('eventos_corporativos', 'listar:todos')
                self.cache_manager.invalidate('eventos_corporativos')  # Invalidar todo o namespace por segurança
                
                return True
            else:
                self.logger.warning(f"Evento não encontrado para atualização: {codigo} - {tipo_evento} em {data}")
                return False
                
        except (sqlite3.Error, ValueError) as e:
            self.logger.error(f"Erro ao atualizar fator do evento: {e}")
            return False
    
    def fechar_conexao(self) -> None:
        """
        Fecha a conexão com o banco de dados.
        """
        if self.conn:
            self.conn.close()
            self.conn = None
            self.cursor = None
            self.logger.info("Conexão com o banco de dados fechada")