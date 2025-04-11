import os
import json
import sqlite3
import pandas as pd
from typing import Dict, List, Tuple

# Importações adicionais para otimização
from fii_utils.cache_manager import get_cache_manager, cached, CachePolicy
from fii_utils.db_decorators import (
    ensure_connection, retry_on_db_locked, log_execution_time
)

from fii_utils.db_utils import conectar_banco
from fii_utils.logging_manager import get_logger

class ExportacaoCotacoesManager:
    """
    Gerencia a exportação de cotações de FIIs selecionados para arquivo Excel.
    Permite selecionar FIIs específicos e lidar com mudanças de ticker.
    """
    
    def __init__(self, arquivo_db: str = 'fundos_imobiliarios.db'):
        self.arquivo_db = arquivo_db
        self.conn = None
        self.cursor = None
        self.logger = get_logger('FIIDatabase')
        
        # Inicializar sistema de cache
        self.cache_manager = get_cache_manager()
        
        # Registrar políticas de cache específicas
        self.cache_manager.register_policy('exportacao_fiis', CachePolicy(ttl=1800, max_size=50))  # 30 minutos
        self.cache_manager.register_policy('exportacao_eventos', CachePolicy(ttl=1800, max_size=100))  # 30 minutos
    
    def conectar(self) -> None:
        """
        Conecta ao banco de dados existente.
        """
        self.conn, self.cursor = conectar_banco(self.arquivo_db)
    
    @ensure_connection
    @cached('exportacao_fiis', key_func=lambda self, arquivo_json: f'fundos:{arquivo_json}')
    def carregar_fundos_json(self, arquivo_json: str) -> Tuple[List[str], Dict[str, str]]:
        """
        Carrega a lista de fundos de um arquivo JSON e prepara para consulta.
        
        Args:
            arquivo_json: Caminho para o arquivo JSON contendo a lista de fundos
            
        Returns:
            Tupla (lista_sql, mapeamento)
                - lista_sql: Lista expandida de todos os tickers para consulta
                - mapeamento: Dicionário mapeando tickers antigos para atuais
        """
        try:
            with open(arquivo_json, 'r', encoding='utf-8') as f:
                dados = json.load(f)
                
            # Validar que o arquivo tem o formato esperado
            if 'fundos' not in dados or not isinstance(dados['fundos'], list):
                raise ValueError("Formato inválido: arquivo JSON deve conter uma chave 'fundos' com uma lista")
                
            lista_fundos = dados['fundos']
            self.logger.info(f"Carregados {len(lista_fundos)} fundos/grupos do arquivo {arquivo_json}")
            
            # Preparar lista para SQL e mapeamento
            lista_sql = []
            mapeamento = {}
            
            for item in lista_fundos:
                if isinstance(item, list):
                    # Para fundos com mudança de ticker
                    for ticker in item:
                        lista_sql.append(ticker)
                    
                    # O último ticker da lista é o atual
                    ticker_atual = item[-1]
                    for ticker in item[:-1]:
                        mapeamento[ticker] = ticker_atual
                else:
                    # Para fundos sem mudança de ticker
                    lista_sql.append(item)
            
            self.logger.info(f"Lista expandida para {len(lista_sql)} tickers para consulta")
            if mapeamento:
                self.logger.info(f"Criado mapeamento para {len(mapeamento)} tickers antigos")
                
            return lista_sql, mapeamento
            
        except (json.JSONDecodeError, ValueError) as e:
            self.logger.error(f"Erro ao carregar arquivo JSON {arquivo_json}: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Erro inesperado ao processar arquivo JSON: {e}")
            raise
    
    @ensure_connection
    @cached('exportacao_eventos', key_func=lambda self, lista_tickers: f'eventos:{",".join(sorted(lista_tickers))}')
    def _carregar_eventos(self, lista_tickers: List[str]) -> List[Dict]:
        """
        Carrega eventos corporativos para os tickers especificados.
        
        Args:
            lista_tickers: Lista de tickers para buscar eventos
        
        Returns:
            Lista de eventos ordenados cronologicamente
        """
        try:
            # Cria placeholders para a consulta SQL
            placeholders = ','.join(['?' for _ in lista_tickers])
            
            # Consulta para buscar eventos dos FIIs na lista
            query = f"""
            SELECT codigo, data, tipo_evento, fator
            FROM eventos_corporativos
            WHERE codigo IN ({placeholders})
            ORDER BY data
            """
            
            # Executa a consulta
            self.cursor.execute(query, lista_tickers)
            rows = self.cursor.fetchall()
            
            # Converte os resultados para uma lista de dicionários
            eventos = []
            for row in rows:
                eventos.append({
                    'codigo': row[0],
                    'data': row[1],
                    'tipo_evento': row[2],
                    'fator': row[3]
                })
            
            self.logger.info(f"Carregados {len(eventos)} eventos corporativos para ajuste de preços")
            return eventos
            
        except sqlite3.Error as e:
            self.logger.error(f"Erro ao carregar eventos corporativos: {e}")
            return []

    def _ajustar_precos(self, df: pd.DataFrame, eventos: List[Dict], colunas_ajuste: List[str]) -> pd.DataFrame:
        """
        Ajusta os preços históricos com base nos eventos corporativos.
        
        Args:
            df: DataFrame com as cotações
            eventos: Lista de eventos corporativos
            colunas_ajuste: Lista de colunas de preço/volume para ajustar
            
        Returns:
            DataFrame com os preços ajustados
        """
        # Se não houver eventos ou o DataFrame estiver vazio, retorna o mesmo DataFrame
        if not eventos or df.empty:
            return df
        
        # Cria uma cópia do DataFrame para não modificar o original
        df_ajustado = df.copy()
        
        # Para cada evento, aplicar o ajuste em ordem cronológica
        for evento in eventos:
            codigo = evento['codigo']
            data_evento = pd.to_datetime(evento['data'])
            tipo_evento = evento['tipo_evento']
            fator = evento['fator']
            
            # Verifica se o código está presente no DataFrame (pode ser um ticker antigo mapeado)
            codigos_para_ajustar = [codigo]
            
            # Aplicar o ajuste apenas em datas anteriores ao evento
            mascara_datas = df_ajustado.index < data_evento
            
            # Para cada coluna que precisa ser ajustada
            for coluna_base in colunas_ajuste:
                # Para cada código que precisa ser ajustado
                for codigo_atual in codigos_para_ajustar:
                    # Constrói o nome da coluna no formato multilevel, se aplicável
                    if isinstance(df_ajustado.columns, pd.MultiIndex):
                        # Para DataFrame com MultiIndex nas colunas
                        for col in df_ajustado.columns:
                            if col[0] == codigo_atual and col[1] == coluna_base:
                                if tipo_evento == 'grupamento':
                                    # Preço aumenta no grupamento (multiplicar pelo fator)
                                    df_ajustado.loc[mascara_datas, col] = df_ajustado.loc[mascara_datas, col] * fator
                                elif tipo_evento == 'desdobramento':
                                    # Preço diminui no desdobramento (dividir pelo fator)
                                    df_ajustado.loc[mascara_datas, col] = df_ajustado.loc[mascara_datas, col] / fator
                    else:
                        # Para DataFrame com colunas simples (apenas fechamento)
                        if codigo_atual in df_ajustado.columns:
                            if tipo_evento == 'grupamento':
                                df_ajustado.loc[mascara_datas, codigo_atual] = df_ajustado.loc[mascara_datas, codigo_atual] * fator
                            elif tipo_evento == 'desdobramento':
                                df_ajustado.loc[mascara_datas, codigo_atual] = df_ajustado.loc[mascara_datas, codigo_atual] / fator
            
            self.logger.info(f"Aplicado evento {tipo_evento} (fator: {fator}) para {codigo} em {data_evento.strftime('%Y-%m-%d')}")
        
        return df_ajustado

    @ensure_connection
    @log_execution_time
    def exportar_cotacoes(self, arquivo_json: str, arquivo_saida: str, dados_completos: bool = False, ajustar_precos: bool = False) -> bool:
        """
        Exporta cotações dos FIIs listados no arquivo JSON para um arquivo Excel.
        
        Args:
            arquivo_json: Caminho para o arquivo JSON com a lista de fundos
            arquivo_saida: Caminho para o arquivo Excel de saída
            dados_completos: Se True, exporta todos os dados (abertura, máxima, mínima, fechamento, volume)
                             Se False, exporta apenas o fechamento
            ajustar_precos: Se True, ajusta os preços históricos com base nos eventos corporativos
            
        Returns:
            True se a exportação foi bem-sucedida, False caso contrário
        """
        try:
            # Carregar e processar a lista de fundos
            lista_sql, mapeamento = self.carregar_fundos_json(arquivo_json)
            
            # Verificar lista vazia
            if not lista_sql:
                self.logger.warning(f"Nenhum FII encontrado no arquivo {arquivo_json}")
                return False
            
            # Construir a consulta SQL com base nos dados solicitados
            placeholders = ','.join(['?' for _ in lista_sql])
            
            if dados_completos:
                # Consulta para todos os dados de cotação
                query = f"""
                SELECT data, codigo, abertura, maxima, minima, fechamento, volume
                FROM cotacoes
                WHERE codigo IN ({placeholders})
                ORDER BY data, codigo
                """
            else:
                # Consulta apenas para fechamento
                query = f"""
                SELECT data, codigo, fechamento
                FROM cotacoes
                WHERE codigo IN ({placeholders})
                ORDER BY data, codigo
                """
            
            # Executar a consulta e carregar os resultados
            self.logger.info(f"Executando consulta para {len(lista_sql)} tickers")
            
            # Usa retry_on_db_locked decorador inline para essa operação específica
            @retry_on_db_locked(max_retries=3, delay_seconds=2)
            def executar_consulta():
                return pd.read_sql_query(query, self.conn, params=lista_sql, parse_dates=['data'])
                
            df_raw = executar_consulta()
            
            # Verificar tickers encontrados vs. solicitados
            tickers_encontrados = set(df_raw['codigo'].unique())
            tickers_nao_encontrados = set(lista_sql) - tickers_encontrados
            
            self.logger.info(f"Encontrados {len(tickers_encontrados)} tickers no banco")
            if tickers_nao_encontrados:
                self.logger.warning(f"Tickers não encontrados no banco: {sorted(tickers_nao_encontrados)}")
            
            # Verificar se há dados para processar
            if df_raw.empty:
                self.logger.warning("Nenhum dado encontrado para os FIIs solicitados")
                return False
            
            # Aplicar mapeamento de tickers antigos para atuais
            df_raw['codigo_atual'] = df_raw['codigo'].map(lambda x: mapeamento.get(x, x))
            
            # Criar DataFrame pivotado de acordo com os dados solicitados
            if dados_completos:
                # Pivot para todos os dados, criando um MultiIndex nas colunas
                df_pivotado = pd.pivot_table(
                    df_raw,
                    index='data',
                    columns=['codigo_atual'],
                    values=['abertura', 'maxima', 'minima', 'fechamento', 'volume'],
                    aggfunc='first'  # Em caso de duplicidade, usa o primeiro valor
                )
                
                # Reordenar o MultiIndex para ter (codigo, valor) em vez de (valor, codigo)
                df_pivotado = df_pivotado.swaplevel(0, 1, axis=1).sort_index(axis=1)
            else:
                # Pivot apenas para fechamento
                df_pivotado = pd.pivot_table(
                    df_raw,
                    index='data', 
                    columns='codigo_atual', 
                    values='fechamento', 
                    aggfunc='first'
                )
                
                # Ordenar colunas alfabeticamente
                df_pivotado = df_pivotado.sort_index(axis=1)
            
            # Ajustar preços históricos se solicitado
            if ajustar_precos:
                # Carregar eventos corporativos usando o método cacheado
                eventos = self._carregar_eventos(lista_sql)
                
                # Verificar se temos eventos para processar
                if eventos:
                    # Ordenar eventos por data para garantir a aplicação cronológica
                    eventos.sort(key=lambda x: x['data'])
                    
                    # Definir quais colunas precisam ser ajustadas
                    if dados_completos:
                        colunas_ajuste = ['abertura', 'maxima', 'minima', 'fechamento']
                    else:
                        colunas_ajuste = ['fechamento']  # Aqui 'fechamento' é apenas um indicador
                    
                    # Aplicar ajustes aos preços
                    df_pivotado = self._ajustar_precos(df_pivotado, eventos, colunas_ajuste)
                    self.logger.info("Preços ajustados com base nos eventos corporativos")
                else:
                    self.logger.info("Nenhum evento corporativo encontrado para ajuste de preços")
            
            # Gerar estatísticas para log
            periodo_inicio = df_pivotado.index.min().strftime('%Y-%m-%d') if not df_pivotado.empty else "N/A"
            periodo_fim = df_pivotado.index.max().strftime('%Y-%m-%d') if not df_pivotado.empty else "N/A"
            
            num_colunas = len(df_pivotado.columns.levels[0]) if dados_completos and isinstance(df_pivotado.columns, pd.MultiIndex) else len(df_pivotado.columns)
            self.logger.info(f"DataFrame resultante com {len(df_pivotado)} datas e {num_colunas} FIIs")
            self.logger.info(f"Período dos dados: {periodo_inicio} a {periodo_fim}")
            
            # Preparar nome do arquivo com sufixo indicando o tipo de dados
            nome_base, extensao = os.path.splitext(arquivo_saida)
            tipo_dados = "_completo" if dados_completos else "_fechamento"
            tipo_ajuste = "_ajustado" if ajustar_precos else ""
            novo_nome = f"{nome_base}{tipo_dados}{tipo_ajuste}{extensao}"
            
            # Exportar para Excel
            if dados_completos:
                # Para dados completos, usar um writer para formatar melhor o Excel
                with pd.ExcelWriter(novo_nome, engine='openpyxl') as writer:
                    df_pivotado.to_excel(writer, sheet_name='Cotacoes')
                    
                    # Adicionar uma segunda aba com metadados
                    metadados = pd.DataFrame({
                        'Descrição': ['Tipo de Dados', 'Ajuste de Preços', 'Período Início', 'Período Fim', 'Total de FIIs', 'Total de Datas'],
                        'Valor': ['Completos (A/M/m/F/V)', 'Sim' if ajustar_precos else 'Não', periodo_inicio, periodo_fim, num_colunas, len(df_pivotado)]
                    })
                    metadados.to_excel(writer, sheet_name='Metadados', index=False)
            else:
                # Para apenas fechamento, exportar o DataFrame direto
                df_pivotado.to_excel(novo_nome)
            
            self.logger.info(f"Dados exportados com sucesso para {novo_nome}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Erro ao exportar cotações: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
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