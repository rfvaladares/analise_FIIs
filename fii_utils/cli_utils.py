"""
Utilitários para interface de linha de comando.
Centraliza funções de interação com o usuário, formatação de saída e processamento de argumentos.
"""

import os
import re
import argparse
import datetime
from typing import List, Tuple, Optional, Any


def imprimir_titulo(titulo: str, largura: int = 50, caractere: str = "=") -> None:
    """
    Imprime um título formatado no console.
    
    Args:
        titulo: Texto do título
        largura: Largura total da linha de separação
        caractere: Caractere usado para a linha de separação
    """
    print("\n" + caractere * largura)
    print(titulo.upper())
    print(caractere * largura)


def imprimir_subtitulo(subtitulo: str, largura: int = 50, caractere: str = "-") -> None:
    """
    Imprime um subtítulo formatado no console.
    
    Args:
        subtitulo: Texto do subtítulo
        largura: Largura total da linha de separação
        caractere: Caractere usado para a linha de separação
    """
    print("\n" + caractere * largura)
    print(subtitulo)
    print(caractere * largura)


def imprimir_item(descricao: str, valor: Any, padding: int = 20) -> None:
    """
    Imprime um item de informação formatado.
    
    Args:
        descricao: Descrição do item
        valor: Valor do item
        padding: Espaçamento entre descrição e valor
    """
    print(f"{descricao + ':':<{padding}} {valor}")


def imprimir_erro(mensagem: str) -> None:
    """
    Imprime uma mensagem de erro formatada.
    
    Args:
        mensagem: Mensagem de erro
    """
    print(f"Erro: {mensagem}")


def imprimir_aviso(mensagem: str) -> None:
    """
    Imprime uma mensagem de aviso formatada.
    
    Args:
        mensagem: Mensagem de aviso
    """
    print(f"Aviso: {mensagem}")


def imprimir_sucesso(mensagem: str) -> None:
    """
    Imprime uma mensagem de sucesso formatada.
    
    Args:
        mensagem: Mensagem de sucesso
    """
    print(f"Sucesso: {mensagem}")


def configurar_argumentos_comuns(parser: argparse.ArgumentParser) -> None:
    """
    Configura argumentos comuns em um parser de argumentos.
    
    Args:
        parser: Parser de argumentos a ser configurado
    """
    # Parâmetros globais
    parser.add_argument('--diretorio', type=str, default='historico_cotacoes',
                        help='Diretório onde estão os arquivos de cotações (padrão: historico_cotacoes)')
    parser.add_argument('--db', type=str, default='fundos_imobiliarios.db',
                        help='Nome do arquivo de banco de dados (padrão: fundos_imobiliarios.db)')
    parser.add_argument('--workers', type=int, default=None,
                        help='Número de workers para processamento paralelo (padrão: metade dos cores disponíveis)')


def configurar_argumentos_download(parser: argparse.ArgumentParser) -> None:
    """
    Configura argumentos específicos para download em um parser.
    
    Args:
        parser: Parser de argumentos a ser configurado
    """
    parser.add_argument('--data', nargs='+', 
                      help='Datas específicas para download. Formatos aceitos: DD/MM/AAAA (dia), MM/AAAA (mês) ou AAAA (ano)')
    parser.add_argument('--range', nargs=2, metavar=('DATA_INICIO', 'DATA_FIM'),
                      help='Intervalo de datas para download. Formatos aceitos: DD/MM/AAAA-DD/MM/AAAA (dias), MM/AAAA-MM/AAAA (meses) ou AAAA-AAAA (anos)')
    parser.add_argument('--anterior', action='store_true',
                      help='Baixa dados do dia útil anterior à data atual')
    parser.add_argument('--limpar-certs', action='store_true',
                      help='Limpa certificados SSL antigos')
    parser.add_argument('--verificar', action='store_true',
                      help='Executa verificações de segurança do ambiente')
    parser.add_argument('--fix-permissions', action='store_true',
                      help='Corrige permissões de diretórios (sistemas Unix/Linux)')
    parser.add_argument('--auto', action='store_true',
                      help='Determina automaticamente quais arquivos baixar com base no banco')
    parser.add_argument('--atualizar', action='store_true',
                      help='Atualiza o banco de dados após o download')
    parser.add_argument('--force', action='store_true',
                      help='Força o download mesmo que o arquivo já exista localmente')
    parser.add_argument('--verificar-zips', action='store_true',
                      help='Verifica se há arquivos ZIP pendentes que precisam ser extraídos')


def configurar_argumentos_exportacao(parser: argparse.ArgumentParser) -> None:
    """
    Configura argumentos específicos para exportação em um parser.
    
    Args:
        parser: Parser de argumentos a ser configurado
    """
    parser.add_argument('--json', type=str, required=True,
                       help='Arquivo JSON com a lista de FIIs para exportar')
    parser.add_argument('--saida', type=str, required=True,
                       help='Caminho para o arquivo Excel de saída')
    parser.add_argument('--completo', action='store_true',
                       help='Exporta dados completos (abertura, máxima, mínima, fechamento, volume)')
    parser.add_argument('--ajustar', action='store_true',
                       help='Aplica ajustes de preço baseados em eventos corporativos')


def processar_argumentos_data(args) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str]], List[str]]:
    """
    Processa argumentos de data e retorna listas apropriadas para cada tipo de download.
    
    Args:
        args: Argumentos da linha de comando
        
    Returns:
        Tupla (datas_diarias, datas_mensais, datas_anuais)
            - datas_diarias: Lista de tuplas (dia, mes, ano) para download diário
            - datas_mensais: Lista de tuplas (mes, ano) para download mensal
            - datas_anuais: Lista de anos para download anual
    """
    datas_diarias = []
    datas_mensais = []
    datas_anuais = []
    
    # Nenhum argumento de data fornecido
    if not hasattr(args, 'data') or args.data is None:
        return datas_diarias, datas_mensais, datas_anuais
    
    # Processa cada string de data
    for data_str in args.data:
        # Formato DD/MM/AAAA (diário)
        if re.match(r'^\d{2}/\d{2}/\d{4}$', data_str):
            dia, mes, ano = data_str.split('/')
            datas_diarias.append((dia, mes, ano))
            
        # Formato MM/AAAA (mensal)
        elif re.match(r'^\d{2}/\d{4}$', data_str):
            mes, ano = data_str.split('/')
            datas_mensais.append((mes, ano))
            
        # Formato AAAA (anual)
        elif re.match(r'^\d{4}$', data_str):
            datas_anuais.append(data_str)
            
        # Formato inválido
        else:
            imprimir_erro(f"Formato de data não reconhecido: {data_str}")
            imprimir_aviso("Formatos aceitos: DD/MM/AAAA (dia), MM/AAAA (mês) ou AAAA (ano)")
    
    return datas_diarias, datas_mensais, datas_anuais


def processar_argumentos_range(args) -> Tuple[Optional[Tuple[datetime.datetime, datetime.datetime]], 
                                             Optional[Tuple[int, int, int, int]], 
                                             Optional[Tuple[int, int]]]:
    """
    Processa argumentos de intervalo de datas (range).
    
    Args:
        args: Argumentos da linha de comando
        
    Returns:
        Tupla (range_diario, range_mensal, range_anual), onde:
        - range_diario: Tupla (data_inicio, data_fim) para download diário
        - range_mensal: Tupla (mes_inicio, ano_inicio, mes_fim, ano_fim) para download mensal
        - range_anual: Tupla (ano_inicio, ano_fim) para download anual
    """
    range_diario = None
    range_mensal = None
    range_anual = None
    
    # Nenhum argumento de range fornecido
    if not hasattr(args, 'range') or args.range is None or len(args.range) != 2:
        return range_diario, range_mensal, range_anual
    
    data_inicio, data_fim = args.range
    
    # Formato DD/MM/AAAA-DD/MM/AAAA (diário)
    if (re.match(r'^\d{2}/\d{2}/\d{4}$', data_inicio) and 
        re.match(r'^\d{2}/\d{2}/\d{4}$', data_fim)):
        try:
            inicio = datetime.datetime.strptime(data_inicio, '%d/%m/%Y')
            fim = datetime.datetime.strptime(data_fim, '%d/%m/%Y')
            range_diario = (inicio, fim)
        except ValueError:
            imprimir_erro(f"Datas inválidas para intervalo diário: {data_inicio}-{data_fim}")
    
    # Formato MM/AAAA-MM/AAAA (mensal)
    elif (re.match(r'^\d{2}/\d{4}$', data_inicio) and 
          re.match(r'^\d{2}/\d{4}$', data_fim)):
        try:
            mes_inicio, ano_inicio = data_inicio.split('/')
            mes_fim, ano_fim = data_fim.split('/')
            range_mensal = (int(mes_inicio), int(ano_inicio), int(mes_fim), int(ano_fim))
        except ValueError:
            imprimir_erro(f"Datas inválidas para intervalo mensal: {data_inicio}-{data_fim}")
    
    # Formato AAAA-AAAA (anual)
    elif (re.match(r'^\d{4}$', data_inicio) and 
          re.match(r'^\d{4}$', data_fim)):
        try:
            range_anual = (int(data_inicio), int(data_fim))
        except ValueError:
            imprimir_erro(f"Anos inválidos para intervalo anual: {data_inicio}-{data_fim}")
    
    # Formato inválido
    else:
        imprimir_erro(f"Formato de intervalo não reconhecido: {data_inicio} a {data_fim}")
        imprimir_aviso("Formatos aceitos para intervalos:")
        imprimir_aviso("  - Diário: DD/MM/AAAA DD/MM/AAAA")
        imprimir_aviso("  - Mensal: MM/AAAA MM/AAAA")
        imprimir_aviso("  - Anual: AAAA AAAA")
    
    return range_diario, range_mensal, range_anual


def calcular_workers(args) -> int:
    """
    Calcula o número de workers com base nos argumentos fornecidos.
    
    Args:
        args: Argumentos da linha de comando
        
    Returns:
        int: Número de workers a ser utilizado
    """
    if hasattr(args, 'workers') and args.workers is not None:
        return args.workers
    
    # Se não foi especificado, usa metade dos cores disponíveis
    return max(1, os.cpu_count() // 2)