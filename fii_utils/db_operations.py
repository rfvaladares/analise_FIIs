"""
Operações comuns de banco de dados para o sistema de análise de FIIs.
Centraliza funções relacionadas ao processamento e estatísticas do banco de dados.
"""

import os
import logging
from typing import Dict, Tuple, Optional, Any
from contextlib import contextmanager

from db_managers.cotacoes import CotacoesManager
from db_managers.arquivos import ArquivosProcessadosManager
from db_managers.eventos import EventosCorporativosManager


@contextmanager
def gerenciador_contexto(gerenciador):
    """
    Context manager para garantir que a conexão com o banco seja fechada corretamente.
    
    Args:
        gerenciador: Uma instância de gerenciador de banco de dados
        
    Yields:
        O gerenciador recebido, após garantir que esteja conectado
    """
    try:
        # Garantir que o gerenciador está conectado
        if hasattr(gerenciador, 'conectar') and gerenciador.conn is None:
            gerenciador.conectar()
        yield gerenciador
    finally:
        # Garantir que a conexão é fechada após o uso
        if hasattr(gerenciador, 'fechar_conexao'):
            gerenciador.fechar_conexao()


def exibir_estatisticas(cotacoes_manager: Optional[CotacoesManager] = None, 
                       arquivos_manager: Optional[ArquivosProcessadosManager] = None, 
                       eventos_manager: Optional[EventosCorporativosManager] = None,
                       console_only: bool = False,
                       db_path: Optional[str] = None) -> Dict:
    """
    Exibe estatísticas sobre os dados no banco.
    
    Args:
        cotacoes_manager: Instância do CotacoesManager
        arquivos_manager: Instância do ArquivosProcessadosManager 
        eventos_manager: Instância opcional do EventosCorporativosManager
        console_only: Se True, apenas exibe no console sem retornar estatísticas
        db_path: Caminho para o arquivo do banco (usar se os gerenciadores não forem fornecidos)
        
    Returns:
        Dicionário com as estatísticas (se console_only=False)
    """
    logger = logging.getLogger('FIIDatabase')
    close_managers = []
    
    try:
        # Criar gerenciadores se não fornecidos mas db_path está disponível
        if db_path:
            if not cotacoes_manager:
                cotacoes_manager = CotacoesManager(db_path)
                cotacoes_manager.conectar()
                close_managers.append(cotacoes_manager)
                
            if not arquivos_manager:
                arquivos_manager = ArquivosProcessadosManager(db_path)
                arquivos_manager.conectar()
                close_managers.append(arquivos_manager)
                
            if not eventos_manager:
                eventos_manager = EventosCorporativosManager(db_path)
                eventos_manager.conectar()
                close_managers.append(eventos_manager)
        
        # Verifica se os gerenciadores necessários estão disponíveis
        if not cotacoes_manager or not arquivos_manager:
            logger.error("Gerenciadores necessários não estão disponíveis para exibir estatísticas")
            return {} if not console_only else None
        
        # Obtém estatísticas de cotações
        stats = cotacoes_manager.obter_estatisticas()
        
        # Obtém lista de arquivos processados
        arquivos = arquivos_manager.listar_arquivos_processados()
        
        # Agrupa por tipo
        stats_tipo = {}
        for arquivo in arquivos:
            tipo = arquivo['tipo']
            if tipo not in stats_tipo:
                stats_tipo[tipo] = {'count': 0, 'registros': 0}
            
            stats_tipo[tipo]['count'] += 1
            stats_tipo[tipo]['registros'] += arquivo['registros_adicionados']
        
        # Exibe as estatísticas
        print("\n" + "="*50)
        print("ESTATÍSTICAS DO BANCO DE DADOS DE FUNDOS IMOBILIÁRIOS")
        print("="*50)
        print(f"Total de registros: {stats['total_registros']:,}")
        print(f"Total de FIIs: {stats['total_fiis']}")
        print(f"Período de dados: {stats['data_minima']} a {stats['data_maxima']}")
        
        print("\nArquivos processados por tipo:")
        for tipo, info in stats_tipo.items():
            print(f"  {tipo}: {info['count']} arquivos, {info['registros']:,} registros")
        
        # Adiciona estatísticas de eventos se o gerenciador foi fornecido
        eventos_stats = {}
        if eventos_manager:
            eventos = eventos_manager.listar_eventos()
            eventos_por_tipo = {}
            
            for e in eventos:
                tipo = e['evento']
                if tipo not in eventos_por_tipo:
                    eventos_por_tipo[tipo] = 0
                eventos_por_tipo[tipo] += 1
            
            print("\nEventos corporativos:")
            print(f"- Total de eventos: {len(eventos)}")
            
            for tipo, count in eventos_por_tipo.items():
                print(f"- {tipo}: {count} eventos")
                
            eventos_stats = {
                'total': len(eventos),
                'por_tipo': eventos_por_tipo
            }
        
        print("="*50)
        
        if not console_only:
            # Retorna um dicionário com todas as estatísticas
            result = {
                'cotacoes': stats,
                'arquivos': {
                    'total': len(arquivos),
                    'por_tipo': stats_tipo
                }
            }
            
            if eventos_stats:
                result['eventos'] = eventos_stats
                
            return result
        
    finally:
        # Fecha conexões de gerenciadores que foram criados aqui
        for manager in close_managers:
            if hasattr(manager, 'fechar_conexao'):
                manager.fechar_conexao()


def verificar_conectar_gerenciadores(db_path: str, logger: logging.Logger, 
                                    include_cotacoes: bool = True,
                                    include_arquivos: bool = True,
                                    include_eventos: bool = False,
                                    num_workers: Optional[int] = None) -> Tuple[Any, ...]:
    """
    Verifica se o banco existe, instancia e conecta gerenciadores.
    
    Args:
        db_path: Caminho para o arquivo do banco de dados
        logger: Logger para registro de eventos
        include_cotacoes: Se deve incluir o gerenciador de cotações
        include_arquivos: Se deve incluir o gerenciador de arquivos
        include_eventos: Se deve incluir o gerenciador de eventos
        num_workers: Número de workers para processamento paralelo
        
    Returns:
        Tupla de gerenciadores conectados (na ordem: cotacoes, arquivos, eventos)
        Os gerenciadores não solicitados serão None
    """
    # Verifica se o banco existe
    if not os.path.exists(db_path):
        logger.error(f"Banco de dados {db_path} não encontrado")
        return (None, None, None)
    
    # Determina número de workers se não foi especificado
    if num_workers is None:
        num_workers = max(1, os.cpu_count() // 2)
    
    # Instancia e conecta os gerenciadores solicitados
    cotacoes_manager = None
    arquivos_manager = None
    eventos_manager = None
    
    try:
        if include_cotacoes:
            cotacoes_manager = CotacoesManager(db_path, num_workers=num_workers)
            cotacoes_manager.conectar()
            
        if include_arquivos:
            arquivos_manager = ArquivosProcessadosManager(db_path)
            arquivos_manager.conectar()
            
        if include_eventos:
            eventos_manager = EventosCorporativosManager(db_path)
            eventos_manager.conectar()
            
        return (cotacoes_manager, arquivos_manager, eventos_manager)
        
    except Exception as e:
        # Em caso de erro, fecha conexões que foram abertas
        logger.error(f"Erro ao conectar gerenciadores: {e}")
        
        if cotacoes_manager and hasattr(cotacoes_manager, 'fechar_conexao'):
            cotacoes_manager.fechar_conexao()
            
        if arquivos_manager and hasattr(arquivos_manager, 'fechar_conexao'):
            arquivos_manager.fechar_conexao()
            
        if eventos_manager and hasattr(eventos_manager, 'fechar_conexao'):
            eventos_manager.fechar_conexao()
            
        return (None, None, None)


def fechar_gerenciadores(cotacoes_manager: Optional[CotacoesManager] = None,
                       arquivos_manager: Optional[ArquivosProcessadosManager] = None,
                       eventos_manager: Optional[EventosCorporativosManager] = None) -> None:
    """
    Fecha conexões dos gerenciadores de maneira segura.
    
    Args:
        cotacoes_manager: Instância do CotacoesManager
        arquivos_manager: Instância do ArquivosProcessadosManager
        eventos_manager: Instância do EventosCorporativosManager
    """
    # Fecha as conexões em ordem reversa para evitar problemas
    if eventos_manager and hasattr(eventos_manager, 'fechar_conexao'):
        eventos_manager.fechar_conexao()
        
    if arquivos_manager and hasattr(arquivos_manager, 'fechar_conexao'):
        arquivos_manager.fechar_conexao()
        
    if cotacoes_manager and hasattr(cotacoes_manager, 'fechar_conexao'):
        cotacoes_manager.fechar_conexao()


def criar_tabelas_banco(db_path: str, logger: logging.Logger) -> bool:
    """
    Cria as tabelas no banco de dados.
    
    Args:
        db_path: Caminho para o arquivo do banco de dados
        logger: Logger para registro de eventos
        
    Returns:
        bool: True se as tabelas foram criadas com sucesso, False caso contrário
    """
    # Instancia gerenciadores
    cotacoes_manager, arquivos_manager, eventos_manager = verificar_conectar_gerenciadores(
        db_path, logger, include_cotacoes=True, include_arquivos=True, include_eventos=True
    )
    
    if not cotacoes_manager or not arquivos_manager or not eventos_manager:
        logger.error("Não foi possível conectar aos gerenciadores para criar tabelas")
        return False
    
    try:
        # Cria as tabelas
        with gerenciador_contexto(cotacoes_manager) as cm:
            cm.criar_tabela()
            
        with gerenciador_contexto(arquivos_manager) as am:
            am.criar_tabela()
            
        with gerenciador_contexto(eventos_manager) as em:
            em.criar_tabela()
        
        logger.info("Tabelas criadas com sucesso")
        return True
        
    except Exception as e:
        logger.error(f"Erro ao criar tabelas: {e}")
        return False
        
    finally:
        # Fecha conexões
        fechar_gerenciadores(cotacoes_manager, arquivos_manager, eventos_manager)