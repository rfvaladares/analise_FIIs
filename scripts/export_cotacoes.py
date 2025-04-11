"""
Script para exportar cotações de FIIs selecionados para arquivo Excel.
Processa arquivo JSON com lista de FIIs e exporta os preços de fechamento.
"""

import os
import sys
import argparse

# Ajusta o path para importar os módulos do pacote
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Substituição da importação do sistema antigo pelo novo
from fii_utils.logging_manager import get_logger
from fii_utils.cli_utils import (
    configurar_argumentos_exportacao, imprimir_titulo, 
    imprimir_item, imprimir_erro, imprimir_sucesso
)
from db_managers.exportacao import ExportacaoCotacoesManager

def main():
    """
    Função principal para exportar cotações de FIIs para Excel.
    """
    # Configura o parser de argumentos
    parser = argparse.ArgumentParser(description='Exporta cotações de FIIs selecionados para arquivo Excel.')
    
    # Usa a função utilitária para configurar argumentos
    configurar_argumentos_exportacao(parser)
    
    # Adiciona argumento específico para banco de dados
    parser.add_argument('--db', type=str, default='fundos_imobiliarios.db',
                        help='Nome do arquivo de banco de dados (padrão: fundos_imobiliarios.db)')
    
    args = parser.parse_args()
    
    # Configura o logger usando o novo sistema
    logger = get_logger('fii_exportacao', console=True, file=True)
    
    # Verifica se o banco de dados existe
    if not os.path.exists(args.db):
        logger.error(f"Banco de dados {args.db} não encontrado")
        imprimir_erro(f"Banco de dados {args.db} não encontrado")
        sys.exit(1)
    
    # Verifica se o arquivo JSON existe
    if not os.path.exists(args.json):
        logger.error(f"Arquivo JSON {args.json} não encontrado")
        imprimir_erro(f"Arquivo JSON {args.json} não encontrado")
        sys.exit(1)
    
    # Instancia e conecta ao gerenciador de exportação
    exportacao_manager = ExportacaoCotacoesManager(args.db)
    
    try:
        # Conecta ao banco de dados
        exportacao_manager.conectar()
        
        # Prepara descrições para feedback ao usuário
        tipo_dados = "completos (abertura, máxima, mínima, fechamento, volume)" if args.completo else "de fechamento"
        tipo_ajuste = "ajustados" if args.ajustar else "não ajustados"
        
        # Imprime título da operação
        imprimir_titulo(f"Exportação de Cotações de FIIs")
        
        # Exporta as cotações com as opções especificadas
        imprimir_item("Tipo de dados", tipo_dados)
        imprimir_item("Ajuste de preços", "Sim" if args.ajustar else "Não")
        imprimir_item("Arquivo de saída", args.saida)
        
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
            imprimir_titulo("Estatísticas do arquivo exportado", caractere="-")
            try:
                # A leitura varia dependendo do tipo de dados exportados
                import pandas as pd
                if args.completo:
                    df = pd.read_excel(nome_arquivo_final, sheet_name='Cotacoes', index_col=0, header=[0, 1])
                    imprimir_item("Total de FIIs", len(df.columns.levels[0].unique()))
                else:
                    df = pd.read_excel(nome_arquivo_final, index_col=0)
                    imprimir_item("Total de FIIs", len(df.columns))
                
                imprimir_item("Período", f"{df.index.min().strftime('%Y-%m-%d')} a {df.index.max().strftime('%Y-%m-%d')}")
                imprimir_item("Total de datas", len(df))
                imprimir_item("Tipo de dados", tipo_dados)
                imprimir_item("Ajuste de preços", "Sim" if args.ajustar else "Não")
            except Exception as e:
                logger.error(f"Erro ao ler estatísticas do arquivo exportado: {e}")
                imprimir_erro("Não foi possível exibir estatísticas detalhadas do arquivo exportado.")
        else:
            imprimir_erro("Erro ao exportar cotações. Verifique o log para mais detalhes.")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Erro durante a exportação: {e}")
        import traceback
        logger.error(traceback.format_exc())
        imprimir_erro(f"{e}")
        sys.exit(1)
    finally:
        # Fecha a conexão com o banco de dados
        exportacao_manager.fechar_conexao()

if __name__ == "__main__":
    main()