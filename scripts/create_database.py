"""
Script para criar o banco de dados de Fundos Imobiliários.
Processa arquivos de cotações históricos da B3, extraindo dados de FIIs.
"""

import os
import sys
import argparse

# Ajusta o path para importar os módulos do pacote
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importações adicionais para otimização
from fii_utils.cache_manager import get_cache_manager
from fii_utils.db_decorators import log_execution_time

# Substituído a importação do logger antigo pelo novo sistema
from fii_utils.logging_manager import get_logger
from fii_utils.arquivo_utils import identificar_arquivos, processar_arquivo
from fii_utils.cli_utils import (
    imprimir_titulo, imprimir_subtitulo, imprimir_item, 
    imprimir_erro, imprimir_sucesso, imprimir_aviso,
    configurar_argumentos_comuns, calcular_workers
)
from fii_utils.db_operations import (
    exibir_estatisticas, fechar_gerenciadores
)
from fii_utils.zip_utils import verificar_extrair_zips_pendentes, obter_arquivos_processados_do_banco
from db_managers.cotacoes import CotacoesManager
from db_managers.arquivos import ArquivosProcessadosManager
from db_managers.eventos import EventosCorporativosManager

def main():
    """
    Função principal para criar o banco de dados de fundos imobiliários
    e processar os arquivos de cotações históricas.
    """
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Cria banco de dados SQLite com cotações de Fundos Imobiliários.')
    
    # Usa a função utilitária para configurar argumentos comuns
    configurar_argumentos_comuns(parser)
    
    args = parser.parse_args()
    
    # Configura o logger usando o novo sistema
    logger = get_logger('fii_db_creation', console=True, file=True)
    
    # Inicializa o cache manager - importante para otimização
    cache_manager = get_cache_manager()
    
    # Verifica se o diretório existe
    if not os.path.isdir(args.diretorio):
        logger.error(f"Diretório não encontrado: {args.diretorio}")
        imprimir_erro(f"Diretório {args.diretorio} não encontrado")
        sys.exit(1)
    
    # Configura o número de workers usando a função utilitária
    args.workers = calcular_workers(args)
    
    # Verifica e extrai arquivos ZIP pendentes
    if os.path.exists(args.db):
        arquivos_processados = obter_arquivos_processados_do_banco(args.db, logger)
    else:
        arquivos_processados = set()
    
    config = {"data_dir": args.diretorio, "extract_retries": 3, "extract_retry_delay": 2.0}
    verificar_extrair_zips_pendentes(args.diretorio, logger, arquivos_processados, config)
    
    imprimir_titulo("Criação do Banco de Dados de Fundos Imobiliários")
    imprimir_item("Banco de dados", args.db)
    imprimir_item("Diretório de dados", args.diretorio)
    imprimir_item("Workers para processamento", args.workers)
    
    # Instancia os gerenciadores
    cotacoes_manager = CotacoesManager(args.db, num_workers=args.workers)
    arquivos_manager = ArquivosProcessadosManager(args.db)
    eventos_manager = EventosCorporativosManager(args.db)
    
    try:
        # Cria as tabelas usando a função centralizada
        cotacoes_manager.conectar()
        arquivos_manager.conectar()
        eventos_manager.conectar()
        
        cotacoes_manager.criar_tabela()
        arquivos_manager.criar_tabela()
        eventos_manager.criar_tabela()
        
        imprimir_sucesso("Tabelas criadas/verificadas com sucesso")
        
        # Processa os arquivos
        processar_arquivos(args.diretorio, cotacoes_manager, arquivos_manager, logger)
        
        # Exibe estatísticas usando a função centralizada
        exibir_estatisticas(cotacoes_manager, arquivos_manager, eventos_manager)
        
    except Exception as e:
        logger.error(f"Erro durante o processamento: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"Erro durante o processamento: {e}")
    finally:
        # Fecha conexões
        fechar_gerenciadores(cotacoes_manager, arquivos_manager, eventos_manager)

@log_execution_time
def processar_arquivos(diretorio, cotacoes_manager, arquivos_manager, logger):
    """
    Processa todos os arquivos de cotação identificados no diretório.
    
    Args:
        diretorio: Diretório onde buscar os arquivos
        cotacoes_manager: Gerenciador de cotações
        arquivos_manager: Gerenciador de arquivos processados
        logger: Logger para registro de eventos
    
    Returns:
        Tuple: (arquivos_processados, registros_inseridos)
    """
    # Identifica os arquivos usando a função centralizada
    arquivos = identificar_arquivos(diretorio, logger)
    
    if not arquivos:
        logger.warning("Nenhum arquivo encontrado para processamento")
        imprimir_aviso("Nenhum arquivo encontrado para processamento")
        return (0, 0)
    
    imprimir_subtitulo(f"Processamento de Arquivos ({len(arquivos)} arquivos)")
    
    total_registros = 0
    processados = 0
    
    # Processa os arquivos
    for i, arquivo in enumerate(arquivos, 1):
        imprimir_item(f"Processando [{i}/{len(arquivos)}]", arquivo.nome_arquivo)
        
        # Usa a função centralizada para processar o arquivo
        registros = processar_arquivo(arquivo, cotacoes_manager, arquivos_manager, logger)
        
        if registros > 0:
            total_registros += registros
            processados += 1
            
            # Commit após cada arquivo processado com sucesso para evitar perda de dados
            if cotacoes_manager.conn:
                cotacoes_manager.conn.commit()
    
    if processados > 0:
        imprimir_sucesso(f"Processamento concluído. {processados} arquivos processados, {total_registros:,} registros inseridos")
    else:
        imprimir_aviso("Nenhum arquivo foi processado com sucesso")
        
    return (processados, total_registros)

if __name__ == "__main__":
    main()