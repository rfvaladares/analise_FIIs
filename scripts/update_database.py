"""
Script para atualizar o banco de dados de Fundos Imobiliários.
Processa novos arquivos de cotações ou arquivos modificados desde a última execução.
Utiliza os módulos centralizados para maior consistência e reuso de código.
"""

import os
import sys
import argparse
import signal

# Ajusta o path para importar os módulos do pacote
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importações de módulos centralizados
from fii_utils.logging_manager import get_logger
from fii_utils.arquivo_utils import identificar_arquivos_novos_modificados, processar_arquivo
from fii_utils.cli_utils import (
    imprimir_titulo, imprimir_subtitulo, imprimir_item, 
    imprimir_erro, imprimir_sucesso, imprimir_aviso,
    configurar_argumentos_comuns, calcular_workers
)
from fii_utils.db_operations import (
    exibir_estatisticas, fechar_gerenciadores, verificar_conectar_gerenciadores
)
from fii_utils.zip_utils import (
    verificar_extrair_zips_pendentes, obter_arquivos_processados_do_banco
)

# Classe para gerenciar os managers e substituir a variável global
class ManagerRegistry:
    """
    Classe para gerenciar as instâncias dos gerenciadores de banco de dados.
    Substitui o uso de variáveis globais por um mecanismo mais seguro.
    """
    
    def __init__(self):
        self.cotacoes_manager = None
        self.arquivos_manager = None
        self.eventos_manager = None
        
    def register(self, cotacoes_manager, arquivos_manager, eventos_manager):
        """
        Registra os gerenciadores de banco de dados.
        
        Args:
            cotacoes_manager: Instância do CotacoesManager
            arquivos_manager: Instância do ArquivosProcessadosManager
            eventos_manager: Instância do EventosCorporativosManager
        """
        self.cotacoes_manager = cotacoes_manager
        self.arquivos_manager = arquivos_manager
        self.eventos_manager = eventos_manager
        
    def close_all(self):
        """
        Fecha as conexões de todos os gerenciadores registrados.
        """
        fechar_gerenciadores(
            self.cotacoes_manager, 
            self.arquivos_manager, 
            self.eventos_manager
        )
        
    def get_managers(self):
        """
        Retorna os gerenciadores registrados.
        
        Returns:
            Tupla (cotacoes_manager, arquivos_manager, eventos_manager)
        """
        return (self.cotacoes_manager, self.arquivos_manager, self.eventos_manager)

# Instância única do registry
manager_registry = ManagerRegistry()

def signal_handler(sig: int, frame) -> None:
    """
    Tratador de sinais para SIGINT (Ctrl+C) e SIGTERM.
    
    Args:
        sig: Número do sinal recebido
        frame: Frame atual da execução
    """
    logger = get_logger('FIIDatabase')
    logger.info(f"Sinal recebido: {sig}. Finalizando graciosamente...")
    
    imprimir_aviso(f"Operação interrompida pelo usuário. Finalizando graciosamente...")
    
    # Fechar conexões usando o registry
    manager_registry.close_all()
    
    sys.exit(0)


def main() -> None:
    """
    Função principal para atualizar o banco de dados de fundos imobiliários
    com novos arquivos de cotações históricas, usando controle de hash.
    """
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(
        description='Atualiza banco de dados SQLite com novas cotações de Fundos Imobiliários.'
    )
    
    # Usa a função utilitária para configurar argumentos comuns
    configurar_argumentos_comuns(parser)
    
    # Argumentos específicos para atualização
    parser.add_argument('--verificar-zips', action='store_true',
                       help='Verifica e extrai arquivos ZIP pendentes antes de atualizar')
    
    args = parser.parse_args()
    
    # Configura o logger usando o novo sistema unificado
    logger = get_logger('FIIDatabase')
    
    # Verifica se o banco de dados existe
    if not os.path.exists(args.db):
        logger.error(f"Banco de dados {args.db} não encontrado. Execute 'criar' primeiro.")
        imprimir_erro(f"Banco de dados {args.db} não encontrado. Execute 'criar' primeiro.")
        sys.exit(1)
    
    # Verifica se o diretório existe
    if not os.path.isdir(args.diretorio):
        logger.error(f"Diretório não encontrado: {args.diretorio}")
        imprimir_erro(f"Diretório {args.diretorio} não encontrado")
        sys.exit(1)
    
    # Calcula o número de workers usando a função utilitária
    args.workers = calcular_workers(args)
    
    # Verificar e extrair ZIPs pendentes se solicitado
    if args.verificar_zips:
        arquivos_processados = obter_arquivos_processados_do_banco(args.db, logger)
        config = {"data_dir": args.diretorio, "extract_retries": 3, "extract_retry_delay": 2.0}
        processados, falhas = verificar_extrair_zips_pendentes(args.diretorio, logger, arquivos_processados, config)
        
        if processados > 0:
            imprimir_sucesso(f"Extraídos {processados} arquivos ZIP pendentes")
        if falhas > 0:
            imprimir_aviso(f"Falha ao extrair {falhas} arquivos ZIP pendentes")
    
    imprimir_titulo("Atualização do Banco de Dados de Fundos Imobiliários")
    imprimir_item("Banco de dados", args.db)
    imprimir_item("Diretório de dados", args.diretorio)
    imprimir_item("Workers para processamento", args.workers)
    
    # Registra os tratadores de sinais para finalização graceful
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Conecta e inicializa os gerenciadores usando a função centralizada
    cotacoes_manager, arquivos_manager, _ = verificar_conectar_gerenciadores(
        args.db, 
        logger, 
        include_cotacoes=True,
        include_arquivos=True,
        include_eventos=False,
        num_workers=args.workers
    )
    
    # Se algum gerenciador não foi conectado, aborta a execução
    if not cotacoes_manager or not arquivos_manager:
        imprimir_erro("Não foi possível conectar aos gerenciadores. Verifique o log para mais detalhes.")
        sys.exit(1)
    
    # Registra os gerenciadores no registry
    manager_registry.register(cotacoes_manager, arquivos_manager, None)
    
    try:
        # Exibe informações sobre a última atualização
        ultima_data = cotacoes_manager.obter_ultima_data()
        if ultima_data:
            logger.info(f"Última data de cotação no banco: {ultima_data}")
            imprimir_item("Última data de cotação", ultima_data)
        
        # Atualiza o banco
        atualizar_banco(args.diretorio, cotacoes_manager, arquivos_manager, logger)
        
        # Exibe estatísticas usando a função centralizada
        exibir_estatisticas(cotacoes_manager, arquivos_manager)
        
    except Exception as e:
        logger.error(f"Erro durante a atualização: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"Erro durante a atualização: {e}")
    finally:
        # Fecha conexões usando o registry
        manager_registry.close_all()


def atualizar_banco(diretorio: str, cotacoes_manager, arquivos_manager, logger) -> None:
    """
    Identifica arquivos novos ou modificados e atualiza o banco de dados.
    
    Args:
        diretorio: Diretório onde buscar os arquivos
        cotacoes_manager: Instância do CotacoesManager
        arquivos_manager: Instância do ArquivosProcessadosManager
        logger: Logger para registro de eventos
    """
    # Usa a função centralizada para identificar arquivos novos/modificados
    arquivos_para_processar = identificar_arquivos_novos_modificados(diretorio, arquivos_manager, logger)
    
    if not arquivos_para_processar:
        logger.info("Nenhum arquivo novo ou modificado encontrado para processamento")
        imprimir_aviso("Nenhum arquivo novo ou modificado encontrado para processamento")
        return
    
    imprimir_subtitulo(f"Processamento de Arquivos ({len(arquivos_para_processar)} arquivos)")
    
    total_registros = 0
    processados = 0
    
    # Processa os arquivos um por um para evitar conflitos de conexão
    for i, (arquivo, foi_modificado) in enumerate(arquivos_para_processar, 1):
        status = "modificado" if foi_modificado else "novo"
        imprimir_item(f"Processando [{i}/{len(arquivos_para_processar)}]", f"{arquivo.nome_arquivo} ({status})")
        
        try:
            # Usa a função centralizada para processar o arquivo
            registros = processar_arquivo(
                arquivo, cotacoes_manager, arquivos_manager, logger, 
                substituir_existentes=foi_modificado
            )
            
            if registros > 0:
                total_registros += registros
                processados += 1
                
            # Importante: faça commit e libere recursos após cada arquivo
            cotacoes_manager.conn.commit()
                
        except Exception as e:
            logger.error(f"Erro ao processar arquivo {arquivo.nome_arquivo}: {e}")
            imprimir_erro(f"Erro ao processar {arquivo.nome_arquivo}: {e}")
            # Continue para o próximo arquivo em caso de erro
            import traceback
            logger.error(traceback.format_exc())
    
    if processados > 0:
        imprimir_sucesso(f"Atualização concluída. {processados} arquivos processados, {total_registros:,} registros inseridos")
    else:
        imprimir_aviso("Nenhum arquivo foi processado com sucesso")


if __name__ == "__main__":
    main()