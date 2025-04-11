"""
Programa principal para o sistema de análise de Fundos Imobiliários.
Integra funcionalidades de criação, atualização, gerenciamento de eventos corporativos, 
exportação e download de dados.

Versão refatorada para usar os gerenciadores centralizados, proporcionando maior
consistência, reuso de código e manutenibilidade.
"""

import os
import argparse
import pandas as pd
import datetime
import sys
from typing import Dict, List, Tuple, Optional, Set, Any

# Módulos utilitários centralizados
from fii_utils.logging_manager import setup_main_logger, get_logger
from fii_utils.config_manager import get_config_manager
from fii_utils.calendar_manager import get_calendar_manager
from fii_utils.cache_manager import get_cache_manager, CachePolicy
from fii_utils.arquivo_utils import (
    identificar_arquivos,
    identificar_arquivos_novos_modificados,
    processar_arquivo
)
from fii_utils.zip_utils import (
    verificar_extrair_zips_pendentes, obter_arquivos_processados_do_banco
)
from fii_utils.download_utils import (
    baixar_arquivo_diario, baixar_arquivo_mensal, baixar_arquivo_anual,
    baixar_arquivos_diarios, baixar_arquivos_mensais, baixar_arquivos_anuais,
    baixar_arquivos_auto
)
from fii_utils.db_operations import (
    exibir_estatisticas, verificar_conectar_gerenciadores, 
    fechar_gerenciadores, criar_tabelas_banco
)
from fii_utils.cli_utils import (
    imprimir_titulo, imprimir_subtitulo, imprimir_item, 
    imprimir_erro, imprimir_sucesso, imprimir_aviso,
    processar_argumentos_data, processar_argumentos_range,
    configurar_argumentos_comuns, calcular_workers
)

# Módulos específicos
from fii_utils.downloader import (
    inicializar as inicializar_downloader,
    limpar_certificados_antigos, verificar_seguranca_ambiente,
    baixar_multiplos_arquivos, determinar_arquivos_para_baixar
)

# Gerenciadores de banco de dados
from db_managers.cotacoes import CotacoesManager
from db_managers.arquivos import ArquivosProcessadosManager
from db_managers.eventos import EventosCorporativosManager
from db_managers.exportacao import ExportacaoCotacoesManager


def main() -> None:
    """
    Função principal que integra todas as funcionalidades do sistema.
    """
    # Inicializar gerenciador de configuração
    config_manager = get_config_manager()
    
    # Inicializar gerenciador de cache
    cache_manager = get_cache_manager()
    
    # Registrar políticas de cache específicas para funções críticas
    cache_manager.register_policy('cotacoes_lista', CachePolicy(ttl=3600, max_size=100))  # 1 hora
    cache_manager.register_policy('cotacoes_ultima_data', CachePolicy(ttl=600, max_size=10))  # 10 minutos
    cache_manager.register_policy('cotacoes_estatisticas', CachePolicy(ttl=1800, max_size=10))  # 30 minutos
    cache_manager.register_policy('arquivos_processados', CachePolicy(ttl=1800, max_size=200))  # 30 minutos
    cache_manager.register_policy('eventos_corporativos', CachePolicy(ttl=3600, max_size=100))  # 1 hora
    
    # Configura o logger principal
    logger = setup_main_logger()
    
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(
        description='Sistema de análise de Fundos Imobiliários - Gerencia banco de dados de cotações e eventos corporativos.'
    )
    
    # Configura argumentos comuns
    configurar_argumentos_comuns(parser)
    
    # Subparsers para diferentes operações
    subparsers = parser.add_subparsers(dest='operacao', help='Operações disponíveis')
    
    # Operação: criar
    parser_criar = subparsers.add_parser('criar', help='Cria o banco de dados e processa arquivos históricos')
    
    # Operação: atualizar
    parser_atualizar = subparsers.add_parser('atualizar', 
                                            help='Atualiza o banco com novos arquivos ou arquivos modificados')
    parser_atualizar.add_argument('--download', action='store_true',
                                help='Baixa automaticamente novos arquivos da B3 antes de atualizar')
    parser_atualizar.add_argument('--verificar-zips', action='store_true',
                                help='Verifica e extrai arquivos ZIP pendentes antes de atualizar')
    
    # Operação: eventos
    parser_eventos = subparsers.add_parser('eventos', 
                                         help='Gerencia eventos corporativos de Fundos Imobiliários')
    
    # Subparsers para operações de eventos
    eventos_subparsers = parser_eventos.add_subparsers(dest='evento_cmd', help='Comandos de eventos')
    
    # Comando: criar tabela
    parser_eventos_criar = eventos_subparsers.add_parser('criar', 
                                                       help='Cria a tabela de eventos corporativos')
    
    # Comando: importar eventos
    parser_eventos_importar = eventos_subparsers.add_parser('importar', 
                                                          help='Importa eventos corporativos')
    parser_eventos_importar.add_argument('--arquivo', type=str, required=True,
                                       help='Arquivo JSON com eventos para importar')
    
    # Comando: listar eventos
    parser_eventos_listar = eventos_subparsers.add_parser('listar', 
                                                        help='Lista eventos corporativos')
    parser_eventos_listar.add_argument('--codigo', type=str, 
                                     help='Filtrar por código do FII')
    
    # Operação: exportar
    parser_exportar = subparsers.add_parser('exportar', 
                                          help='Exporta cotações de FIIs selecionados para arquivo Excel')
    parser_exportar.add_argument('--json', type=str, required=True,
                               help='Arquivo JSON com a lista de FIIs para exportar')
    parser_exportar.add_argument('--saida', type=str, required=True,
                               help='Caminho para o arquivo Excel de saída')
    parser_exportar.add_argument('--completo', action='store_true',
                               help='Exporta dados completos (abertura, máxima, mínima, fechamento, volume)')
    parser_exportar.add_argument('--ajustar', action='store_true',
                               help='Aplica ajustes de preço baseados em eventos corporativos')
    
    # Operação: download
    parser_download = subparsers.add_parser('download',
                                         help='Baixa arquivos históricos de cotações da B3')
    
    # Configuração dos argumentos de download
    parser_download.add_argument('--data', nargs='+', 
                              help='Datas específicas para download. Formatos aceitos: DD/MM/AAAA (dia), MM/AAAA (mês) ou AAAA (ano)')
    parser_download.add_argument('--range', nargs=2, metavar=('DATA_INICIO', 'DATA_FIM'),
                              help='Intervalo de datas para download. Formatos aceitos: DD/MM/AAAA-DD/MM/AAAA (dias), MM/AAAA-MM/AAAA (meses) ou AAAA-AAAA (anos)')
    parser_download.add_argument('--anterior', action='store_true',
                              help='Baixa dados do dia útil anterior à data atual')
    parser_download.add_argument('--limpar-certs', action='store_true',
                              help='Limpa certificados SSL antigos')
    parser_download.add_argument('--verificar', action='store_true',
                              help='Executa verificações de segurança do ambiente')
    parser_download.add_argument('--fix-permissions', action='store_true',
                              help='Corrige permissões de diretórios (sistemas Unix/Linux)')
    parser_download.add_argument('--auto', action='store_true',
                              help='Determina automaticamente quais arquivos baixar com base no banco')
    parser_download.add_argument('--atualizar', action='store_true',
                              help='Atualiza o banco de dados após o download')
    parser_download.add_argument('--force', action='store_true',
                              help='Força o download mesmo que o arquivo já exista localmente')
    parser_download.add_argument('--verificar-zips', action='store_true',
                              help='Verifica se há arquivos ZIP pendentes que precisam ser extraídos')
    
    # Operação: info
    parser_info = subparsers.add_parser('info', 
                                      help='Exibe informações sobre o banco de dados')
    
    # Operação: extrair
    parser_extrair = subparsers.add_parser('extrair',
                                        help='Verifica e extrai arquivos ZIP pendentes')
    
    # Operação: cache (Nova)
    parser_cache = subparsers.add_parser('cache', help='Gerencia sistema de cache')
    parser_cache.add_argument('--clear', action='store_true', help='Limpa todo o cache')
    parser_cache.add_argument('--stats', action='store_true', help='Exibe estatísticas do cache')
    parser_cache.add_argument('--invalidate', type=str, help='Invalida namespace específico')
    
    # Processa os argumentos
    args = parser.parse_args()
    
    # Atualiza a configuração com argumentos da linha de comando
    if args.diretorio:
        config_manager.update("data_dir", args.diretorio)
    if args.db:
        config_manager.update("db_path", args.db)
    
    # Configura workers
    args.workers = calcular_workers(args)
    
    # Registra o início da operação
    logger.info(f"Iniciando operação: {args.operacao if hasattr(args, 'operacao') else 'info'}")
    
    # Executa a operação selecionada
    if args.operacao == 'criar':
        criar_banco(args, logger)
    elif args.operacao == 'atualizar':
        # Verificar ZIPs pendentes se solicitado ou se --download foi especificado
        if args.verificar_zips or args.download:
            verificar_extrair_zips(args.diretorio, logger, args.db)
        
        if args.download:
            # Se --download foi especificado, baixar arquivos antes de atualizar
            config_manager = get_config_manager()
            config = config_manager.get_config()
            baixar_arquivos_auto(args, config, args.db, logger)
        
        atualizar_banco(args, logger)
    elif args.operacao == 'eventos':
        gerenciar_eventos(args, logger)
    elif args.operacao == 'exportar':
        exportar_cotacoes(args, logger)
    elif args.operacao == 'download':
        # Verificar e extrair ZIPs pendentes se solicitado
        if args.verificar_zips:
            verificar_extrair_zips(args.diretorio, logger, args.db)
        
        baixar_arquivos(args, logger)
        
        # Atualizar banco após download se solicitado
        if args.atualizar:
            # Verificar novamente ZIPs pendentes após download
            verificar_extrair_zips(args.diretorio, logger, args.db)
            args.download = False  # Evitar loop infinito
            atualizar_banco(args, logger)
    elif args.operacao == 'extrair':
        verificar_extrair_zips(args.diretorio, logger, args.db)
    elif args.operacao == 'cache':
        gerenciar_cache(args, logger)
    elif args.operacao == 'info' or not hasattr(args, 'operacao'):
        mostrar_info(args, logger)
    else:
        parser.print_help()


def verificar_extrair_zips(diretorio: str, logger, db_path: str = 'fundos_imobiliarios.db') -> Tuple[int, int]:
    """
    Função wrapper para verificar e extrair ZIPs pendentes.
    
    Args:
        diretorio: Diretório onde buscar arquivos ZIP
        logger: Logger para registro de eventos
        db_path: Caminho para o arquivo do banco de dados
        
    Returns:
        Tupla (processados, falhas) com número de arquivos processados e falhas
    """
    # Obter lista de arquivos já processados do banco
    arquivos_processados = obter_arquivos_processados_do_banco(db_path, logger)
    
    # Obter configuração
    config_manager = get_config_manager()
    
    # Configuração para extração
    config = {
        "data_dir": diretorio, 
        "extract_retries": config_manager.get("extract_retries", 3), 
        "extract_retry_delay": config_manager.get("extract_retry_delay", 2.0)
    }
    
    # Verificar e extrair ZIPs pendentes
    processados, falhas = verificar_extrair_zips_pendentes(diretorio, logger, arquivos_processados, config)
    
    # Mostrar resultado
    if processados > 0:
        imprimir_sucesso(f"Foram extraídos {processados} arquivos ZIP pendentes com sucesso")
    
    if falhas > 0:
        imprimir_aviso(f"Falha ao extrair {falhas} arquivos ZIP pendentes. Verifique o log para mais detalhes")
    
    if processados == 0 and falhas == 0:
        imprimir_item("Status", "Nenhum arquivo ZIP pendente encontrado para extração")
    
    return processados, falhas


def criar_banco(args, logger) -> None:
    """
    Cria o banco de dados e processa arquivos históricos.
    
    Args:
        args: Argumentos da linha de comando
        logger: Logger para registro de eventos
    """
    # Verifica se o diretório existe
    if not os.path.isdir(args.diretorio):
        logger.error(f"Diretório não encontrado: {args.diretorio}")
        imprimir_erro(f"Diretório {args.diretorio} não encontrado")
        return
    
    # Verificar se há ZIPs pendentes antes de processar
    verificar_extrair_zips(args.diretorio, logger, args.db)
    
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
        criar_tabelas_banco(args.db, logger)
        
        # Conecta aos gerenciadores
        cotacoes_manager.conectar()
        arquivos_manager.conectar()
        eventos_manager.conectar()
        
        # Identifica arquivos para processamento usando a função centralizada
        arquivos = identificar_arquivos(args.diretorio, logger)
        
        if not arquivos:
            logger.warning("Nenhum arquivo encontrado para processamento")
            imprimir_aviso("Nenhum arquivo encontrado para processamento")
            return
        
        imprimir_subtitulo(f"Processamento de Arquivos ({len(arquivos)} arquivos)")
        
        # Processa os arquivos usando a função centralizada
        total_registros = 0
        processados = 0
        
        for i, arquivo in enumerate(arquivos, 1):
            imprimir_item(f"Processando [{i}/{len(arquivos)}]", arquivo.nome_arquivo)
            
            # Usa a função centralizada para processar o arquivo
            registros = processar_arquivo(arquivo, cotacoes_manager, arquivos_manager, logger)
            
            if registros > 0:
                total_registros += registros
                processados += 1
        
        if processados > 0:
            imprimir_sucesso(f"Processamento concluído. {processados} arquivos processados, {total_registros:,} registros inseridos")
        else:
            imprimir_aviso("Nenhum arquivo foi processado com sucesso")
        
        # Exibe estatísticas usando a função centralizada
        exibir_estatisticas(cotacoes_manager, arquivos_manager, eventos_manager)
        
        # Exibe estatísticas do cache após a criação
        cache = get_cache_manager()
        exibir_estatisticas_cache(cache, show_details=False)
        
    except Exception as e:
        logger.error(f"Erro ao criar banco de dados: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"Erro ao criar banco de dados: {e}")
    finally:
        # Fecha conexões usando a função centralizada
        fechar_gerenciadores(cotacoes_manager, arquivos_manager, eventos_manager)


def atualizar_banco(args, logger) -> None:
    """
    Atualiza o banco com novos arquivos ou arquivos modificados.
    
    Args:
        args: Argumentos da linha de comando
        logger: Logger para registro de eventos
    """
    # Verifica se o banco existe
    if not os.path.exists(args.db):
        logger.error(f"Banco de dados {args.db} não encontrado. Execute 'criar' primeiro.")
        imprimir_erro(f"Banco de dados {args.db} não encontrado. Execute 'criar' primeiro.")
        return
    
    # Verifica se o diretório existe
    if not os.path.isdir(args.diretorio):
        logger.error(f"Diretório não encontrado: {args.diretorio}")
        imprimir_erro(f"Diretório {args.diretorio} não encontrado")
        return
    
    imprimir_titulo("Atualização do Banco de Dados de Fundos Imobiliários")
    imprimir_item("Banco de dados", args.db)
    imprimir_item("Diretório de dados", args.diretorio)
    imprimir_item("Workers para processamento", args.workers)
    
    # Conecta aos gerenciadores usando a função centralizada
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
        return
    
    try:
        # Exibe a última data do banco
        ultima_data = cotacoes_manager.obter_ultima_data()
        if ultima_data:
            logger.info(f"Última data de cotação no banco: {ultima_data}")
            imprimir_item("Última data de cotação", ultima_data)
        
        # Identifica arquivos novos ou modificados usando a função centralizada
        arquivos_para_processar = identificar_arquivos_novos_modificados(args.diretorio, arquivos_manager, logger)
        
        if not arquivos_para_processar:
            logger.info("Nenhum arquivo novo ou modificado encontrado para processamento")
            imprimir_aviso("Nenhum arquivo novo ou modificado encontrado para processamento")
            return
        
        imprimir_subtitulo(f"Processamento de Arquivos ({len(arquivos_para_processar)} arquivos)")
        
        # Processa os arquivos
        total_registros = 0
        processados = 0
        
        for i, (arquivo, foi_modificado) in enumerate(arquivos_para_processar, 1):
            status = "modificado" if foi_modificado else "novo"
            imprimir_item(f"Processando [{i}/{len(arquivos_para_processar)}]", f"{arquivo.nome_arquivo} ({status})")
            
            # Usa a função centralizada para processar o arquivo
            registros = processar_arquivo(
                arquivo, cotacoes_manager, arquivos_manager, logger, 
                substituir_existentes=foi_modificado
            )
            
            if registros > 0:
                total_registros += registros
                processados += 1
                
                # Commit após cada arquivo processado com sucesso
                cotacoes_manager.conn.commit()
        
        if processados > 0:
            imprimir_sucesso(f"Atualização concluída. {processados} arquivos processados, {total_registros:,} registros inseridos")
        else:
            imprimir_aviso("Nenhum arquivo foi processado com sucesso")
        
        # Exibe estatísticas usando a função centralizada
        exibir_estatisticas(cotacoes_manager, arquivos_manager)
        
        # Exibe estatísticas de cache após a atualização
        cache = get_cache_manager()
        exibir_estatisticas_cache(cache, show_details=False)
        
    except Exception as e:
        logger.error(f"Erro ao atualizar banco de dados: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"Erro ao atualizar banco de dados: {e}")
    finally:
        # Fecha conexões usando a função centralizada
        fechar_gerenciadores(cotacoes_manager, arquivos_manager)


def gerenciar_eventos(args, logger) -> None:
    """
    Gerencia eventos corporativos de Fundos Imobiliários.
    
    Args:
        args: Argumentos da linha de comando
        logger: Logger para registro de eventos
    """
    # Verifica se o banco existe
    if not os.path.exists(args.db):
        logger.error(f"Banco de dados {args.db} não encontrado. Execute 'criar' primeiro.")
        imprimir_erro(f"Banco de dados {args.db} não encontrado. Execute 'criar' primeiro.")
        return
    
    # Instancia o gerenciador de eventos
    _, _, eventos_manager = verificar_conectar_gerenciadores(
        args.db, 
        logger, 
        include_cotacoes=False,
        include_arquivos=False,
        include_eventos=True
    )
    
    if not eventos_manager:
        imprimir_erro("Não foi possível conectar ao gerenciador de eventos. Verifique o log para mais detalhes.")
        return
    
    try:
        # Executa o comando selecionado
        if args.evento_cmd == 'criar':
            # Cria a tabela
            eventos_manager.criar_tabela()
            imprimir_sucesso("Tabela de eventos corporativos criada/verificada com sucesso")
            
        elif args.evento_cmd == 'importar':
            # Importa eventos de arquivo JSON
            if args.arquivo:
                try:
                    import json
                    with open(args.arquivo, 'r', encoding='utf-8') as f:
                        eventos = json.load(f)
                        
                    eventos_manager.criar_tabela()
                    inseridos = eventos_manager.inserir_eventos(eventos)
                    imprimir_sucesso(f"Importados {inseridos} de {len(eventos)} eventos do arquivo {args.arquivo}")
                    
                    # Invalidar cache de eventos após importação massiva
                    cache = get_cache_manager()
                    cache.invalidate('eventos_corporativos')
                    
                except Exception as e:
                    logger.error(f"Erro ao importar eventos: {e}")
                    imprimir_erro(f"Erro ao importar eventos: {e}")
            else:
                imprimir_erro("Arquivo de eventos não especificado (use --arquivo)")
                
        elif args.evento_cmd == 'listar':
            # Lista eventos
            if args.codigo:
                codigo = args.codigo.upper()
                eventos = eventos_manager.listar_eventos(codigo)
                filtro = f"para o código {codigo}"
            else:
                eventos = eventos_manager.listar_eventos()
                filtro = "todos"
            
            # Exibe os resultados
            imprimir_subtitulo(f"Eventos corporativos ({filtro}, total: {len(eventos)})")
            
            print(f"{'CÓDIGO':<10} {'EVENTO':<15} {'DATA':<12} {'FATOR':<8} {'REGISTRO'}")
            print("-" * 70)
            
            for e in eventos:
                print(f"{e['codigo']:<10} {e['evento']:<15} {e['data']:<12} {e['fator']:<8.2f} {e['data_registro']}")
            
            if not eventos:
                print("Nenhum evento encontrado.")
            print("-" * 70)
            
            # Exibe estatísticas do cache após listagem
            cache = get_cache_manager()
            exibir_estatisticas_cache(cache, namespace='eventos_corporativos', show_details=False)
            
        else:
            imprimir_erro("Comando de eventos não especificado. Use criar, importar ou listar.")
            
    except Exception as e:
        logger.error(f"Erro ao gerenciar eventos: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"Erro ao gerenciar eventos: {e}")
    finally:
        # Fecha a conexão
        fechar_gerenciadores(eventos_manager=eventos_manager)


def exportar_cotacoes(args, logger) -> None:
    """
    Exporta cotações de FIIs selecionados para arquivo Excel.
    
    Args:
        args: Argumentos da linha de comando
        logger: Logger para registro de eventos
    """
    # Verifica se o banco existe
    if not os.path.exists(args.db):
        logger.error(f"Banco de dados {args.db} não encontrado")
        imprimir_erro(f"Banco de dados {args.db} não encontrado")
        return
    
    # Verifica se o arquivo JSON existe
    if not os.path.exists(args.json):
        logger.error(f"Arquivo JSON {args.json} não encontrado")
        imprimir_erro(f"Arquivo JSON {args.json} não encontrado")
        return
    
    # Instancia o gerenciador de exportação
    exportacao_manager = ExportacaoCotacoesManager(args.db)
    
    try:
        # Conecta ao banco de dados
        exportacao_manager.conectar()
        
        # Prepara descrições para feedback ao usuário
        tipo_dados = "completos (abertura, máxima, mínima, fechamento, volume)" if args.completo else "de fechamento"
        tipo_ajuste = "ajustados" if args.ajustar else "não ajustados"
        
        imprimir_titulo("Exportação de Cotações de FIIs")
        imprimir_item("Banco de dados", args.db)
        imprimir_item("Arquivo JSON", args.json)
        imprimir_item("Arquivo de saída", args.saida)
        imprimir_item("Tipo de dados", tipo_dados)
        imprimir_item("Ajuste de preços", "Ativado" if args.ajustar else "Desativado")
        
        # Exporta as cotações com as opções especificadas
        logger.info(f"Exportando dados {tipo_dados} {tipo_ajuste} para {args.saida}...")
        sucesso = exportacao_manager.exportar_cotacoes(
            args.json, 
            args.saida, 
            dados_completos=args.completo, 
            ajustar_precos=args.ajustar
        )
        
        if sucesso:
            # Modificar nome do arquivo de saída para refletir as opções escolhidas
            nome_base, extensao = os.path.splitext(args.saida)
            tipo_dados_sufixo = "_completo" if args.completo else "_fechamento"
            tipo_ajuste_sufixo = "_ajustado" if args.ajustar else ""
            nome_arquivo_final = f"{nome_base}{tipo_dados_sufixo}{tipo_ajuste_sufixo}{extensao}"
            
            imprimir_sucesso(f"Cotações exportadas com sucesso para {nome_arquivo_final}")
            
            # Mostrar estatísticas básicas
            imprimir_subtitulo("Estatísticas do arquivo exportado")
            
            try:
                # A leitura varia dependendo do tipo de dados exportados
                if args.completo:
                    df = pd.read_excel(nome_arquivo_final, sheet_name='Cotacoes', index_col=0, header=[0, 1])
                    imprimir_item("Total de FIIs", len(df.columns.levels[0].unique()))
                else:
                    df = pd.read_excel(nome_arquivo_final, index_col=0)
                    imprimir_item("Total de FIIs", len(df.columns))
                
                imprimir_item("Período de dados", f"{df.index.min().strftime('%Y-%m-%d')} a {df.index.max().strftime('%Y-%m-%d')}")
                imprimir_item("Total de datas", len(df))
            except Exception as e:
                logger.error(f"Erro ao ler estatísticas do arquivo exportado: {e}")
                logger.debug(f"Detalhe do erro: {str(e)}")
                imprimir_aviso("Não foi possível exibir estatísticas detalhadas do arquivo exportado")
            
            # Exibe estatísticas do cache após exportação
            cache = get_cache_manager()
            exibir_estatisticas_cache(cache, show_details=False)
            
        else:
            imprimir_erro("Erro ao exportar cotações. Verifique o log para mais detalhes.")
            
    except Exception as e:
        logger.error(f"Erro ao exportar cotações: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"Erro ao exportar cotações: {e}")
    finally:
        # Fecha a conexão com o banco de dados
        if exportacao_manager and hasattr(exportacao_manager, 'fechar_conexao'):
            exportacao_manager.fechar_conexao()


def baixar_arquivos(args, logger) -> None:
    """
    Baixa arquivos históricos de cotações da B3.
    
    Args:
        args: Argumentos da linha de comando
        logger: Logger para registro de eventos
    """
    # Inicializa o downloader
    config_manager = inicializar_downloader()
    
    imprimir_titulo("B3 Downloader - Download de Cotações Históricas")
    
    # Aplicar opções de linha de comando à configuração
    if args.fix_permissions:
        config_manager.update("fix_permissions", True)
    
    # Verificações de segurança e limpeza
    if args.verificar or True:  # Sempre executar verificações
        verificar_seguranca_ambiente()
    
    if args.limpar_certs or True:  # Sempre limpar certificados antigos
        limpar_certificados_antigos()
    
    # Determinar quais arquivos baixar
    datas = []
    download_anual = False
    download_mensal = False
    anos = []
    meses = []
    
    # Obter gerenciador de calendário
    calendar_manager = get_calendar_manager()
    
    if args.auto:
        # Modo automático: determinar baseado no banco de dados
        logger.info("Modo automático: determinando arquivos para baixar com base no banco de dados")
        imprimir_item("Modo de download", "Automático (baseado no banco de dados)")
        
        # Obter a configuração
        config = config_manager.get_config()
        
        # Usar função centralizada para baixar arquivos automaticamente
        if baixar_arquivos_auto(args, config, args.db, logger):
            imprimir_sucesso("Download automático concluído com sucesso")
        else:
            imprimir_aviso("Download automático concluído, mas alguns arquivos podem não ter sido baixados")
            
        return
    
    elif args.data:
        # Usa a função centralizada para processar argumentos de data
        datas_diarias, datas_mensais, datas_anuais = processar_argumentos_data(args)
        
        if datas_diarias:
            datas = datas_diarias
            imprimir_item("Modo de download", f"Diário ({len(datas_diarias)} data(s) específica(s))")
        
        if datas_mensais:
            meses = datas_mensais
            download_mensal = True
            imprimir_item("Modo de download", f"Mensal ({len(datas_mensais)} mês(es) específico(s))")
        
        if datas_anuais:
            anos = datas_anuais
            download_anual = True
            imprimir_item("Modo de download", f"Anual ({len(datas_anuais)} ano(s) específico(s))")
    
    elif args.range:
        # Usa a função centralizada para processar argumentos de range
        range_diario, range_mensal, range_anual = processar_argumentos_range(args)
        
        if range_diario:
            inicio, fim = range_diario
            imprimir_item("Modo de download", f"Diário (intervalo de {inicio.strftime('%d/%m/%Y')} a {fim.strftime('%d/%m/%Y')})")
            datas = baixar_arquivos_diarios(inicio, fim, args.force)
        
        elif range_mensal:
            mes_inicio, ano_inicio, mes_fim, ano_fim = range_mensal
            imprimir_item("Modo de download", f"Mensal (intervalo de {mes_inicio:02d}/{ano_inicio} a {mes_fim:02d}/{ano_fim})")
            meses = baixar_arquivos_mensais(mes_inicio, ano_inicio, mes_fim, ano_fim, args.force)
            download_mensal = True
        
        elif range_anual:
            ano_inicio, ano_fim = range_anual
            imprimir_item("Modo de download", f"Anual (intervalo de {ano_inicio} a {ano_fim})")
            anos = baixar_arquivos_anuais(ano_inicio, ano_fim, args.force)
            download_anual = True
    
    elif args.anterior:
        # Dia útil anterior
        hoje = datetime.datetime.now().date()
        dia_util_anterior = calendar_manager.get_previous_trading_day(hoje)
        
        dia = dia_util_anterior.strftime('%d')
        mes = dia_util_anterior.strftime('%m')
        ano = dia_util_anterior.strftime('%Y')
        
        datas = [(dia, mes, ano)]
        imprimir_item("Modo de download", f"Diário (dia útil anterior: {dia}/{mes}/{ano})")
    
    else:
        # Padrão: usar data atual se for dia útil, caso contrário usa o dia útil anterior
        hoje = datetime.datetime.now().date()
        if calendar_manager.is_trading_day(hoje):
            dia = hoje.strftime('%d')
            mes = hoje.strftime('%m')
            ano = hoje.strftime('%Y')
            imprimir_item("Modo de download", f"Diário (data atual (dia útil): {dia}/{mes}/{ano})")
        else:
            dia_util_anterior = calendar_manager.get_previous_trading_day(hoje)
            dia = dia_util_anterior.strftime('%d')
            mes = dia_util_anterior.strftime('%m')
            ano = dia_util_anterior.strftime('%Y')
            imprimir_item("Modo de download", f"Diário (dia útil anterior: {dia}/{mes}/{ano})")
        
        datas = [(dia, mes, ano)]
    
    # Executar downloads conforme o tipo
    success_count = 0
    file_count = 0
    
    if download_anual and anos:
        imprimir_subtitulo(f"Download de Arquivos Anuais ({len(anos)} anos)")
        
        # Download de arquivos anuais usando a função centralizada
        for ano in anos:
            imprimir_item("Baixando", f"Arquivo anual para {ano}")
            sucesso = baixar_arquivo_anual(ano, args.force)
            if sucesso:
                success_count += 1
                imprimir_sucesso(f"Download do arquivo anual para {ano} concluído com sucesso")
            else:
                imprimir_erro(f"Falha ao baixar arquivo anual para {ano}")
            file_count += 1
    
    if download_mensal and meses:
        imprimir_subtitulo(f"Download de Arquivos Mensais ({len(meses)} meses)")
        
        # Download de arquivos mensais usando a função centralizada
        for mes, ano in meses:
            imprimir_item("Baixando", f"Arquivo mensal para {mes}/{ano}")
            sucesso = baixar_arquivo_mensal(mes, ano, args.force)
            if sucesso:
                success_count += 1
                imprimir_sucesso(f"Download do arquivo mensal para {mes}/{ano} concluído com sucesso")
            else:
                imprimir_erro(f"Falha ao baixar arquivo mensal para {mes}/{ano}")
            file_count += 1
    
    if datas:
        imprimir_subtitulo(f"Download de Arquivos Diários ({len(datas)} datas)")
        
        # Download de arquivos diários
        sucessos, falhas, nao_disponiveis, arquivos_txt = baixar_multiplos_arquivos(datas, args.force)
        success_count += sucessos
        file_count += len(datas)
        
        if sucessos > 0:
            imprimir_sucesso(f"Download de {sucessos} arquivo(s) diário(s) concluído com sucesso")
        if falhas > 0:
            imprimir_erro(f"Falha ao baixar {falhas} arquivo(s) diário(s)")
        if nao_disponiveis > 0:
            imprimir_aviso(f"{nao_disponiveis} arquivo(s) ainda não disponível(is) (normal para datas recentes)")
    
    # Resumo final
    if file_count > 0:
        imprimir_subtitulo("Resumo do Download")
        imprimir_item("Total de arquivos", file_count)
        imprimir_item("Downloads bem-sucedidos", success_count)
        imprimir_item("Downloads com falha", file_count - success_count)
        
        if success_count > 0:
            # Verificar ZIPs pendentes após os downloads
            verificar_extrair_zips(args.diretorio, logger, args.db)
        
        if args.atualizar and success_count > 0:
            imprimir_item("Próxima ação", "Atualizar banco de dados com os arquivos baixados")
            args.download = False  # Evitar loop infinito
            atualizar_banco(args, logger)
    else:
        imprimir_aviso("Nenhuma data selecionada para download")


def mostrar_info(args, logger) -> None:
    """
    Exibe informações sobre o banco de dados.
    
    Args:
        args: Argumentos da linha de comando
        logger: Logger para registro de eventos
    """
    # Verifica se o banco existe
    if not os.path.exists(args.db):
        imprimir_erro(f"Banco de dados {args.db} não encontrado")
        return
    
    imprimir_titulo("Informações do Banco de Dados de Fundos Imobiliários")
    imprimir_item("Banco de dados", args.db)
    
    try:
        # Conecta e inicializa os gerenciadores usando a função centralizada
        cotacoes_manager, arquivos_manager, eventos_manager = verificar_conectar_gerenciadores(
            args.db, 
            logger, 
            include_cotacoes=True,
            include_arquivos=True,
            include_eventos=True
        )
        
        # Se algum gerenciador não foi conectado, exibe erro mas continua com os disponíveis
        if not cotacoes_manager:
            imprimir_erro("Não foi possível conectar ao gerenciador de cotações")
        if not arquivos_manager:
            imprimir_erro("Não foi possível conectar ao gerenciador de arquivos")
        if not eventos_manager:
            imprimir_erro("Não foi possível conectar ao gerenciador de eventos")
        
        # Exibe estatísticas usando a função centralizada
        if cotacoes_manager and arquivos_manager:
            exibir_estatisticas(cotacoes_manager, arquivos_manager, eventos_manager)
        else:
            imprimir_erro("Não foi possível exibir estatísticas completas sem os gerenciadores necessários")
        
        # Exibe estatísticas do cache
        cache = get_cache_manager()
        exibir_estatisticas_cache(cache)
        
    except Exception as e:
        logger.error(f"Erro ao obter informações do banco: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"Erro ao obter informações do banco: {e}")
    finally:
        # Fecha conexões usando a função centralizada
        fechar_gerenciadores(cotacoes_manager, arquivos_manager, eventos_manager)


def gerenciar_cache(args, logger) -> None:
    """
    Gerencia o sistema de cache.
    
    Args:
        args: Argumentos da linha de comando
        logger: Logger para registro de eventos
    """
    cache = get_cache_manager()
    
    imprimir_titulo("Gerenciamento do Sistema de Cache")
    
    if args.clear:
        cache.clear()
        imprimir_sucesso("Cache completamente limpo")
    
    if args.invalidate:
        cache.invalidate(args.invalidate)
        imprimir_sucesso(f"Cache do namespace '{args.invalidate}' invalidado")
    
    if args.stats or (not args.clear and not args.invalidate):
        exibir_estatisticas_cache(cache)


def exibir_estatisticas_cache(cache, namespace=None, show_details=True) -> None:
    """
    Exibe estatísticas do sistema de cache.
    
    Args:
        cache: Instância do CacheManager
        namespace: Namespace específico para exibir estatísticas (opcional)
        show_details: Se deve exibir detalhes completos ou resumo
    """
    stats = cache.get_stats()
    
    if not show_details:
        # Versão resumida para acompanhar outras operações
        if stats['entries'] > 0:
            hit_ratio = stats['hit_ratio'] if 'hit_ratio' in stats else 0
            imprimir_item("Cache", f"{stats['entries']} entradas, {hit_ratio:.1f}% de acertos")
        return
    
    # Versão detalhada para o comando 'cache'
    imprimir_subtitulo("Estatísticas do Sistema de Cache")
    imprimir_item("Total de entradas", stats['entries'])
    imprimir_item("Cache hits", stats['hits'])
    imprimir_item("Cache misses", stats['misses'])
    imprimir_item("Taxa de acerto", f"{stats['hit_ratio']:.2f}%")
    imprimir_item("Evicções", stats['evictions'])
    
    if namespace and 'namespaces' in stats and stats['namespaces']:
        # Exibe estatísticas apenas para o namespace especificado
        if namespace in stats['namespaces']:
            imprimir_item(f"Entradas no namespace '{namespace}'", stats['namespaces'][namespace])
        else:
            imprimir_aviso(f"Namespace '{namespace}' não encontrado no cache")
    elif stats['namespaces']:
        # Exibe estatísticas para todos os namespaces
        imprimir_subtitulo("Entradas por Namespace")
        for ns, count in stats['namespaces'].items():
            imprimir_item(ns, count)


if __name__ == "__main__":
    main()