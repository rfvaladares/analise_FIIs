"""
Script para gerenciar a tabela de eventos corporativos para Fundos Imobiliários.
Permite criar a tabela, inserir, listar, atualizar e remover eventos.
"""

import os
import sys
import argparse
import json

# Ajusta o path para importar os módulos do pacote
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Substituição da importação do sistema antigo pelo novo
from fii_utils.logging_manager import get_logger
from fii_utils.cli_utils import (
    imprimir_titulo, imprimir_subtitulo, imprimir_item, 
    imprimir_erro, imprimir_sucesso, imprimir_aviso
)
from db_managers.eventos import EventosCorporativosManager

def main():
    """
    Função principal para gerenciar a tabela de eventos corporativos.
    """
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Gerencia eventos corporativos de Fundos Imobiliários.')
    parser.add_argument('--db', type=str, default='fundos_imobiliarios.db',
                        help='Nome do arquivo de banco de dados (padrão: fundos_imobiliarios.db)')
    
    # Subparsers para diferentes operações
    subparsers = parser.add_subparsers(dest='comando', help='Comandos disponíveis')
    
    # Comando: criar
    parser_criar = subparsers.add_parser('criar', help='Cria a tabela de eventos corporativos')
    
    # Comando: importar
    parser_importar = subparsers.add_parser('importar', help='Importa eventos corporativos')
    parser_importar.add_argument('--arquivo', type=str, required=True, 
                                help='Arquivo JSON com eventos para importar')
    
    # Comando: adicionar
    parser_adicionar = subparsers.add_parser('adicionar', help='Adiciona um novo evento corporativo')
    parser_adicionar.add_argument('--codigo', type=str, required=True, help='Código do FII')
    parser_adicionar.add_argument('--evento', type=str, required=True, 
                                choices=['grupamento', 'desdobramento'], help='Tipo do evento')
    parser_adicionar.add_argument('--data', type=str, required=True, help='Data do evento (YYYY-MM-DD)')
    parser_adicionar.add_argument('--fator', type=float, required=True, help='Fator do evento')
    
    # Comando: remover
    parser_remover = subparsers.add_parser('remover', help='Remove um evento corporativo')
    parser_remover.add_argument('--codigo', type=str, required=True, help='Código do FII')
    parser_remover.add_argument('--evento', type=str, required=True, 
                              choices=['grupamento', 'desdobramento'], help='Tipo do evento')
    parser_remover.add_argument('--data', type=str, required=True, help='Data do evento (YYYY-MM-DD)')
    
    # Comando: atualizar
    parser_atualizar = subparsers.add_parser('atualizar', help='Atualiza o fator de um evento existente')
    parser_atualizar.add_argument('--codigo', type=str, required=True, help='Código do FII')
    parser_atualizar.add_argument('--evento', type=str, required=True, 
                                choices=['grupamento', 'desdobramento'], help='Tipo do evento')
    parser_atualizar.add_argument('--data', type=str, required=True, help='Data do evento (YYYY-MM-DD)')
    parser_atualizar.add_argument('--fator', type=float, required=True, help='Novo fator do evento')
    
    # Comando: listar
    parser_listar = subparsers.add_parser('listar', help='Lista eventos corporativos')
    parser_listar.add_argument('--codigo', type=str, help='Filtrar por código do FII')
    parser_listar.add_argument('--periodo', type=str, nargs=2, metavar=('DATA_INICIO', 'DATA_FIM'),
                             help='Filtrar por período (YYYY-MM-DD YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    # Configura o logger usando o novo sistema
    logger = get_logger('fii_eventos', console=True, file=True)
    
    # Verificar se o banco de dados existe
    if not os.path.exists(args.db):
        imprimir_erro(f"Banco de dados {args.db} não encontrado")
        return
    
    # Instancia o gerenciador de eventos
    eventos_manager = EventosCorporativosManager(args.db)
    
    try:
        eventos_manager.conectar()
        
        # Executa o comando selecionado
        if args.comando == 'criar':
            cmd_criar_tabela(eventos_manager, logger)
            
        elif args.comando == 'importar':
            cmd_importar_eventos(eventos_manager, args, logger)
            
        elif args.comando == 'adicionar':
            cmd_adicionar_evento(eventos_manager, args, logger)
            
        elif args.comando == 'remover':
            cmd_remover_evento(eventos_manager, args, logger)
            
        elif args.comando == 'atualizar':
            cmd_atualizar_evento(eventos_manager, args, logger)
            
        elif args.comando == 'listar':
            cmd_listar_eventos(eventos_manager, args)
            
        else:
            # Se nenhum comando foi selecionado, exibe a ajuda
            parser.print_help()
            
    except Exception as e:
        logger.error(f"Erro ao executar comando: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"Erro inesperado: {e}")
    finally:
        eventos_manager.fechar_conexao()

def cmd_criar_tabela(eventos_manager, logger):
    """
    Cria a tabela de eventos corporativos.
    """
    imprimir_titulo("Criação da Tabela de Eventos Corporativos")
    
    eventos_manager.criar_tabela()
    logger.info("Tabela de eventos corporativos criada/verificada com sucesso")
    
    imprimir_sucesso("Tabela de eventos corporativos criada/verificada com sucesso")

def cmd_importar_eventos(eventos_manager, args, logger):
    """
    Importa eventos corporativos de arquivo JSON.
    """
    imprimir_titulo("Importação de Eventos Corporativos")
    imprimir_item("Arquivo de origem", args.arquivo)
    
    try:
        with open(args.arquivo, 'r', encoding='utf-8') as f:
            eventos = json.load(f)
        logger.info(f"Carregados {len(eventos)} eventos do arquivo {args.arquivo}")
        imprimir_item("Eventos carregados", len(eventos))
    except Exception as e:
        logger.error(f"Erro ao carregar arquivo {args.arquivo}: {e}")
        imprimir_erro(f"Erro ao carregar arquivo: {e}")
        return
    
    # Cria a tabela se não existir
    eventos_manager.criar_tabela()
    
    # Importa os eventos
    inseridos = eventos_manager.inserir_eventos(eventos)
    logger.info(f"Importados {inseridos} de {len(eventos)} eventos")
    
    imprimir_sucesso(f"Importados {inseridos} de {len(eventos)} eventos com sucesso")

def cmd_adicionar_evento(eventos_manager, args, logger):
    """
    Adiciona um novo evento corporativo.
    """
    imprimir_titulo("Adicionar Evento Corporativo")
    
    # Cria a tabela se não existir
    eventos_manager.criar_tabela()
    
    # Prepara o evento
    evento = {
        'codigo': args.codigo.upper(),
        'evento': args.evento,
        'data': args.data,
        'fator': args.fator
    }
    
    imprimir_item("Código", evento['codigo'])
    imprimir_item("Tipo de evento", evento['evento'])
    imprimir_item("Data", evento['data'])
    imprimir_item("Fator", evento['fator'])
    
    # Tenta inserir o evento
    if eventos_manager.inserir_evento(evento):
        logger.info(f"Evento adicionado: {evento}")
        imprimir_sucesso(f"Evento adicionado com sucesso: {args.codigo} - {args.evento} em {args.data}")
    else:
        imprimir_erro(f"Não foi possível adicionar o evento. Verifique o log para detalhes.")

def cmd_remover_evento(eventos_manager, args, logger):
    """
    Remove um evento corporativo.
    """
    imprimir_titulo("Remover Evento Corporativo")
    
    imprimir_item("Código", args.codigo.upper())
    imprimir_item("Tipo de evento", args.evento)
    imprimir_item("Data", args.data)
    
    if eventos_manager.remover_evento(args.codigo.upper(), args.data, args.evento):
        logger.info(f"Evento removido: {args.codigo} - {args.evento} em {args.data}")
        imprimir_sucesso(f"Evento removido com sucesso: {args.codigo} - {args.evento} em {args.data}")
    else:
        imprimir_erro(f"Não foi possível remover o evento. Verifique se ele existe.")

def cmd_atualizar_evento(eventos_manager, args, logger):
    """
    Atualiza o fator de um evento existente.
    """
    imprimir_titulo("Atualizar Fator de Evento Corporativo")
    
    imprimir_item("Código", args.codigo.upper())
    imprimir_item("Tipo de evento", args.evento)
    imprimir_item("Data", args.data)
    imprimir_item("Novo fator", args.fator)
    
    if eventos_manager.atualizar_fator(args.codigo.upper(), args.data, args.evento, args.fator):
        logger.info(f"Fator atualizado: {args.codigo} - {args.evento} em {args.data}")
        imprimir_sucesso(f"Fator atualizado com sucesso para {args.fator}")
    else:
        imprimir_erro(f"Não foi possível atualizar o evento. Verifique se ele existe.")

def cmd_listar_eventos(eventos_manager, args):
    """
    Lista eventos corporativos, com opções de filtro.
    """
    imprimir_titulo("Listagem de Eventos Corporativos")
    
    eventos = []
    
    if args.periodo:
        # Filtrar por período
        data_inicio, data_fim = args.periodo
        eventos = eventos_manager.obter_eventos_por_periodo(data_inicio, data_fim)
        filtro = f"período de {data_inicio} a {data_fim}"
        imprimir_item("Filtro", f"Período de {data_inicio} a {data_fim}")
    elif args.codigo:
        # Filtrar por código
        codigo = args.codigo.upper()
        eventos = eventos_manager.listar_eventos(codigo)
        filtro = f"código {codigo}"
        imprimir_item("Filtro", f"Código {codigo}")
    else:
        # Sem filtros
        eventos = eventos_manager.listar_eventos()
        filtro = "todos"
        imprimir_item("Filtro", "Todos os eventos")
    
    imprimir_item("Total de eventos", len(eventos))
    
    # Exibe os resultados
    imprimir_subtitulo(f"Eventos Corporativos ({len(eventos)} eventos)")
    
    if eventos:
        print(f"{'CÓDIGO':<10} {'EVENTO':<15} {'DATA':<12} {'FATOR':<8} {'REGISTRO'}")
        print("-" * 70)
        
        for e in eventos:
            print(f"{e['codigo']:<10} {e['evento']:<15} {e['data']:<12} {e['fator']:<8.2f} {e['data_registro']}")
    else:
        imprimir_aviso("Nenhum evento encontrado com os critérios especificados.")

if __name__ == "__main__":
    main()