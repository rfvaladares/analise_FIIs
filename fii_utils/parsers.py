import os
import re
import logging
import traceback
import sys
from datetime import datetime, timedelta
from typing import Dict, Optional, List, Tuple
from multiprocessing import current_process

# Importação do sistema unificado de logging
from fii_utils.logging_manager import get_logger, LoggingManager

class ArquivoCotacao:
    """
    Classe que representa um arquivo de cotação da B3, identificando seu tipo
    e período de dados com base no nome do arquivo.
    """
    # Padrões de regex para os diferentes formatos de arquivo
    PADRAO_ANUAL = re.compile(r'COTAHIST_A(\d{4})\.(TXT|ZIP)')
    PADRAO_DIARIO = re.compile(r'COTAHIST_D(\d{2})(\d{2})(\d{4})\.(TXT|ZIP)')
    PADRAO_MENSAL = re.compile(r'COTAHIST_M(\d{2})(\d{4})\.(TXT|ZIP)')
    
    def __init__(self, caminho_arquivo: str):
        self.caminho = caminho_arquivo
        self.nome_arquivo = os.path.basename(caminho_arquivo)
        self.tipo = None  # 'anual', 'mensal' ou 'diario'
        self.ano = None
        self.mes = None
        self.dia = None
        self.data_inicio = None
        self.data_fim = None
        self.extensao = os.path.splitext(caminho_arquivo)[1].upper()  # .TXT ou .ZIP
        self._analisar_nome_arquivo()
    
    def _analisar_nome_arquivo(self):
        """Analisa o nome do arquivo para determinar seu tipo e período."""
        # Verifica se é arquivo anual
        match = self.PADRAO_ANUAL.match(self.nome_arquivo)
        if match:
            ano = int(match.group(1))
            self.tipo = 'anual'
            self.ano = ano
            self.data_inicio = datetime(ano, 1, 1)
            self.data_fim = datetime(ano, 12, 31)
            return
        
        # Verifica se é arquivo diário
        match = self.PADRAO_DIARIO.match(self.nome_arquivo)
        if match:
            dia = int(match.group(1))
            mes = int(match.group(2))
            ano = int(match.group(3))
            self.tipo = 'diario'
            self.dia = dia
            self.mes = mes
            self.ano = ano
            self.data_inicio = self.data_fim = datetime(ano, mes, dia)
            return
        
        # Verifica se é arquivo mensal
        match = self.PADRAO_MENSAL.match(self.nome_arquivo)
        if match:
            mes = int(match.group(1))
            ano = int(match.group(2))
            self.tipo = 'mensal'
            self.mes = mes
            self.ano = ano
            self.data_inicio = datetime(ano, mes, 1)
            
            # Determina o último dia do mês
            if mes == 12:
                self.data_fim = datetime(ano, 12, 31)
            else:
                next_month = datetime(ano, mes + 1, 1)
                self.data_fim = next_month - timedelta(days=1)
            return
        
        # Se não corresponder a nenhum padrão
        raise ValueError(f"Formato de nome de arquivo não reconhecido: {self.nome_arquivo}")
    
    def __str__(self):
        return f"{self.nome_arquivo} ({self.tipo}: {self.data_inicio.strftime('%d/%m/%Y')} a {self.data_fim.strftime('%d/%m/%Y')})"


class CotacaoParser:
    """
    Classe responsável por fazer o parsing de registros de cotação
    do arquivo de cotações históricas da B3.
    """
    
    def __init__(self):
        # Mapeamento das posições dos campos no registro tipo 01 (cotações)
        # Os índices são ajustados para base 0 em Python (diferente do layout que começa em 1)
        self.campos = {
            'tipo_registro': (0, 2),
            'data_pregao': (2, 10),
            'codbdi': (10, 12),
            'codigo_negociacao': (12, 24),
            'tipo_mercado': (24, 27),
            'nome_empresa': (27, 39),
            'especificacao': (39, 49),
            'preco_abertura': (56, 69),
            'preco_maximo': (69, 82),
            'preco_minimo': (82, 95),
            'preco_medio': (95, 108),  # Mantemos a referência para o campo, mas não o utilizaremos
            'preco_ultimo': (108, 121),
            'preco_melhor_oferta_compra': (121, 134),
            'preco_melhor_oferta_venda': (134, 147),
            'numero_negocios': (147, 152),
            'quantidade_papeis_negociados': (152, 170),
            'volume_total': (170, 188)
        }
    
    def parse_linha(self, linha: str) -> Optional[Dict]:
        """
        Analisa uma linha do arquivo e extrai os campos relevantes
        se for um registro do tipo 01 (cotações) e for um fundo imobiliário.
        """
        # Verifica se o tamanho da linha é compatível com o layout
        if len(linha) < 245:
            return None
        
        # Verifica se é um registro de cotação (tipo 01)
        tipo_registro = linha[self.campos['tipo_registro'][0]:self.campos['tipo_registro'][1]].strip()
        if tipo_registro != '01':
            return None
        
        # Extrai o código BDI para verificar se é fundo imobiliário (12)
        codbdi = linha[self.campos['codbdi'][0]:self.campos['codbdi'][1]].strip()
        if codbdi != '12':
            return None
        
        # Extrai os demais campos relevantes
        data_str = linha[self.campos['data_pregao'][0]:self.campos['data_pregao'][1]].strip()
        data = datetime.strptime(data_str, '%Y%m%d').strftime('%Y-%m-%d')
        
        codigo = linha[self.campos['codigo_negociacao'][0]:self.campos['codigo_negociacao'][1]].strip()
        
        # Converte os valores monetários (formato (11)V99 significa 11 dígitos inteiros e 2 decimais)
        try:
            preco_abertura = self._parse_valor_monetario(linha[self.campos['preco_abertura'][0]:self.campos['preco_abertura'][1]])
            preco_maximo = self._parse_valor_monetario(linha[self.campos['preco_maximo'][0]:self.campos['preco_maximo'][1]])
            preco_minimo = self._parse_valor_monetario(linha[self.campos['preco_minimo'][0]:self.campos['preco_minimo'][1]])
            preco_ultimo = self._parse_valor_monetario(linha[self.campos['preco_ultimo'][0]:self.campos['preco_ultimo'][1]])
            volume_total = self._parse_valor_monetario(linha[self.campos['volume_total'][0]:self.campos['volume_total'][1]])
            qtd_negocios = int(linha[self.campos['numero_negocios'][0]:self.campos['numero_negocios'][1]].strip() or '0')
            qtd_papeis = int(linha[self.campos['quantidade_papeis_negociados'][0]:self.campos['quantidade_papeis_negociados'][1]].strip() or '0')
        except ValueError as e:
            logger = get_logger('FIIDatabase')
            logger.error(f"Erro ao converter valores para o código {codigo} na data {data}: {e}")
            return None
        
        return {
            'data': data,
            'codigo': codigo,
            'abertura': preco_abertura,
            'maxima': preco_maximo,
            'minima': preco_minimo,
            'fechamento': preco_ultimo,
            'volume': volume_total,
            'negocios': qtd_negocios,
            'quantidade': qtd_papeis
        }
    
    def _parse_valor_monetario(self, valor_str: str) -> float:
        """
        Converte o valor monetário do formato da B3 para float.
        O formato (11)V99 significa 11 dígitos inteiros e 2 decimais,
        sem o ponto decimal explícito no arquivo.
        """
        valor_str = valor_str.strip()
        if not valor_str:
            return 0.0
        
        # Remove zeros à esquerda
        valor_str = valor_str.lstrip('0')
        if not valor_str:
            return 0.0
        
        # Converte para float, considerando os 2 últimos dígitos como decimais
        if len(valor_str) == 1:
            return float(f"0.0{valor_str}")
        elif len(valor_str) == 2:
            return float(f"0.{valor_str}")
        else:
            return float(f"{valor_str[:-2]}.{valor_str[-2:]}")


def _configurar_logger_processo() -> logging.Logger:
    """
    Configura o logger específico para o processo atual.
    
    Em um ambiente multiprocesso, cada processo precisa de seu próprio logger
    configurado de forma apropriada para evitar problemas de concorrência.
    
    Returns:
        logging.Logger: Logger configurado para o processo atual
    """
    proc_id = current_process().pid
    proc_name = current_process().name
    logger_name = f"FIIChunkProcess-{proc_id}"
    
    # Usa o sistema unificado de logging para configurar o logger do processo
    proc_logger = LoggingManager.setup(
        logger_name,
        console=True,
        file=True,
        level=logging.INFO
    )
    
    # Registra o início do trabalho deste processo
    proc_logger.info(f"Processo {proc_name} (PID {proc_id}) iniciado")
    
    return proc_logger


def processar_chunk(dados_chunk: Tuple[List[str], CotacaoParser]) -> List[Tuple]:
    """
    Função auxiliar para processar um chunk de linhas em um processo separado.
    Deve ser definida no escopo global para permitir o uso com ProcessPoolExecutor.
    
    Args:
        dados_chunk: Tupla (linhas, parser) onde:
            - linhas: Lista de strings contendo as linhas do arquivo a processar
            - parser: Objeto CotacaoParser para processar as linhas
            
    Returns:
        Lista de tuplas com os dados dos registros processados
    """
    # Configuração do logger específica para este processo
    proc_logger = _configurar_logger_processo()
    
    linhas, parser = dados_chunk
    registros = []
    
    try:
        proc_logger.info(f"Iniciando processamento de chunk com {len(linhas)} linhas")
        
        # Processa as linhas do chunk
        for i, linha in enumerate(linhas):
            registro = parser.parse_linha(linha)
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
            
            # Log de progresso a cada 10.000 linhas
            if i > 0 and i % 10000 == 0:
                proc_logger.info(f"Processadas {i}/{len(linhas)} linhas, encontrados {len(registros)} registros de FIIs")
        
        proc_logger.info(f"Processamento de chunk concluído. Extraídos {len(registros)} registros de FIIs")
        return registros
        
    except Exception as e:
        # Captura e registra qualquer exceção para evitar falhas silenciosas
        error_msg = f"Erro ao processar chunk: {e}"
        stack_trace = traceback.format_exc()
        
        # Tenta usar o logger configurado
        try:
            proc_logger.error(error_msg)
            proc_logger.error(stack_trace)
        except Exception:
            # Se o logger falhar, tenta escrever no stderr como último recurso
            print(f"ERRO CRÍTICO NO PROCESSO {current_process().pid}: {error_msg}", file=sys.stderr)
            print(stack_trace, file=sys.stderr)
        
        # Retorna lista vazia em caso de erro para não interromper todo o processamento
        # O processo principal deve verificar e lidar com chunks vazios
        return []