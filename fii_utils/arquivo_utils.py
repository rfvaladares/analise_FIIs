"""
Utilitários para processamento de arquivos de cotações.
Centraliza funções relacionadas à identificação e manipulação de arquivos.
"""

import os
import logging
from typing import List, Tuple

from fii_utils.parsers import ArquivoCotacao
from fii_utils.logging_manager import get_logger


def normalizar_caminho_arquivo(caminho_arquivo: str, priorizar_zip: bool = True) -> str:
    """
    Normaliza o caminho de um arquivo, convertendo entre versões ZIP e TXT conforme necessário.
    
    Args:
        caminho_arquivo: Caminho completo do arquivo
        priorizar_zip: Se True, prioriza a versão ZIP do arquivo se existir
        
    Returns:
        Caminho normalizado do arquivo
    """
    logger = get_logger('FIIDatabase')
    
    # Obtém o diretório, nome e extensão
    diretorio = os.path.dirname(caminho_arquivo)
    nome_base = os.path.basename(caminho_arquivo)
    
    # Converte nome para maiúsculas para garantir consistência
    nome_base = nome_base.upper()
    
    # Determina a extensão e o nome sem extensão
    if nome_base.endswith('.TXT'):
        extensao = '.TXT'
        nome_sem_ext = nome_base[:-4]
        outra_extensao = '.ZIP'
    elif nome_base.endswith('.ZIP'):
        extensao = '.ZIP'
        nome_sem_ext = nome_base[:-4]
        outra_extensao = '.TXT'
    else:
        # Se não tem extensão reconhecida, retorna o caminho original
        logger.warning(f"Arquivo sem extensão reconhecida: {caminho_arquivo}")
        return caminho_arquivo
    
    # Calcula caminhos para as versões ZIP e TXT
    caminho_zip = os.path.join(diretorio, nome_sem_ext + '.ZIP')
    caminho_txt = os.path.join(diretorio, nome_sem_ext + '.TXT')
    
    # Lógica de priorização
    if priorizar_zip:
        # Prioriza ZIP sobre TXT
        if extensao == '.ZIP' or not os.path.exists(caminho_zip):
            # Se já é ZIP ou o ZIP não existe, mantém o caminho original
            caminho_normalizado = os.path.join(diretorio, nome_sem_ext + extensao)
        else:
            # Se existe versão ZIP e o original é TXT, retorna o caminho do ZIP
            logger.debug(f"Priorizando versão ZIP ({caminho_zip}) sobre TXT ({caminho_txt})")
            caminho_normalizado = caminho_zip
    else:
        # Prioriza TXT sobre ZIP (caso específico)
        if extensao == '.TXT' or not os.path.exists(caminho_txt):
            # Se já é TXT ou o TXT não existe, mantém o caminho original
            caminho_normalizado = os.path.join(diretorio, nome_sem_ext + extensao)
        else:
            # Se existe versão TXT e o original é ZIP, retorna o caminho do TXT
            logger.debug(f"Priorizando versão TXT ({caminho_txt}) sobre ZIP ({caminho_zip})")
            caminho_normalizado = caminho_txt
    
    return caminho_normalizado


def verificar_arquivo_existe(nome_arquivo: str, diretorio: str) -> Tuple[bool, str]:
    """
    Verifica se um arquivo existe no diretório, priorizando a versão ZIP se disponível.
    
    Args:
        nome_arquivo: Nome do arquivo (pode ser com extensão .TXT ou .ZIP)
        diretorio: Diretório onde buscar o arquivo
        
    Returns:
        Tupla (existe, caminho_completo)
    """
    # Normaliza o nome do arquivo para maiúsculas
    nome_arquivo = nome_arquivo.upper()
    
    # Remove extensão se presente
    if nome_arquivo.endswith('.TXT') or nome_arquivo.endswith('.ZIP'):
        nome_base = nome_arquivo[:-4]
    else:
        nome_base = nome_arquivo
    
    # Verifica primeiro a versão ZIP (prioridade)
    caminho_zip = os.path.join(diretorio, nome_base + '.ZIP')
    if os.path.exists(caminho_zip):
        return True, caminho_zip
    
    # Depois verifica a versão TXT
    caminho_txt = os.path.join(diretorio, nome_base + '.TXT')
    if os.path.exists(caminho_txt):
        return True, caminho_txt
    
    # Não encontrou nenhuma versão
    return False, ""


def identificar_arquivos(diretorio: str, logger: logging.Logger) -> List[ArquivoCotacao]:
    """
    Identifica todos os arquivos de cotação no diretório e os ordena.
    Prioriza arquivos ZIP sobre TXT quando ambos existem.
    
    Args:
        diretorio: Diretório onde buscar os arquivos
        logger: Logger para registro de eventos
        
    Returns:
        Lista de objetos ArquivoCotacao ordenados por tipo e data
    """
    arquivos = []
    arquivos_processados = set()  # Para evitar duplicidade ZIP/TXT
    
    # Primeiro passo: listar todos os arquivos no diretório
    todos_arquivos = [f for f in os.listdir(diretorio) 
                     if f.startswith('COTAHIST_') and 
                     (f.endswith('.TXT') or f.endswith('.ZIP'))]
    
    # Organizar por nome base (sem extensão)
    arquivos_por_nome = {}
    for nome_arquivo in todos_arquivos:
        nome_base = nome_arquivo[:-4]  # Remove a extensão (.TXT ou .ZIP)
        if nome_base not in arquivos_por_nome:
            arquivos_por_nome[nome_base] = []
        arquivos_por_nome[nome_base].append(nome_arquivo)
    
    # Processar cada grupo de arquivos, priorizando ZIP sobre TXT
    for nome_base, versoes in arquivos_por_nome.items():
        # Priorizar versão ZIP se disponível
        if any(v.endswith('.ZIP') for v in versoes):
            nome_escolhido = next(v for v in versoes if v.endswith('.ZIP'))
        else:
            nome_escolhido = next(v for v in versoes if v.endswith('.TXT'))
        
        # Cria o objeto ArquivoCotacao
        caminho_completo = os.path.join(diretorio, nome_escolhido)
        try:
            arquivo = ArquivoCotacao(caminho_completo)
            arquivos.append(arquivo)
            # Registra outras versões encontradas
            if len(versoes) > 1:
                logger.info(f"Múltiplas versões encontradas para {nome_base}, usando {nome_escolhido}")
        except ValueError as e:
            logger.warning(f"Arquivo ignorado: {nome_escolhido}. Erro: {e}")
    
    # Ordena os arquivos: primeiro os anuais, depois os mensais, por fim os diários
    arquivos.sort(key=lambda a: (
        0 if a.tipo == 'anual' else (1 if a.tipo == 'mensal' else 2),
        a.data_inicio
    ))
    
    logger.info(f"Encontrados {len(arquivos)} arquivos para processamento")
    return arquivos


def identificar_arquivos_novos_modificados(diretorio: str, arquivos_manager, logger: logging.Logger) -> List[Tuple[ArquivoCotacao, bool]]:
    """
    Identifica arquivos novos ou modificados que precisam ser processados.
    Considera apenas arquivos ZIP para verificação de status de processamento.
    
    Args:
        diretorio: Diretório onde buscar os arquivos
        arquivos_manager: Instância do ArquivosProcessadosManager
        logger: Logger para registro de eventos
        
    Returns:
        Lista de tuplas (ArquivoCotacao, foi_modificado)
    """
    arquivos_para_processar = []
    nomes_processados = set()  # Para evitar processamento duplicado
    
    # Primeiro, identificamos todos os arquivos ZIP disponíveis
    arquivos_zip = []
    for nome_arquivo in os.listdir(diretorio):
        if nome_arquivo.upper().startswith('COTAHIST_') and nome_arquivo.upper().endswith('.ZIP'):
            arquivos_zip.append(nome_arquivo)
    
    logger.info(f"Encontrados {len(arquivos_zip)} arquivos ZIP para análise")
    
    # Para cada arquivo ZIP, verificamos se já foi processado ou se foi modificado
    for nome_zip in arquivos_zip:
        # Normalizamos para maiúsculas para consistência
        nome_zip = nome_zip.upper()
        
        if nome_zip in nomes_processados:
            continue
            
        caminho_completo = os.path.join(diretorio, nome_zip)
        
        try:
            # Verifica se o arquivo já foi processado e se foi modificado
            # Esta verificação considera apenas arquivos ZIP
            processado, modificado = arquivos_manager.verificar_arquivo_processado(caminho_completo)
            
            if not processado:
                logger.info(f"Novo arquivo ZIP encontrado: {nome_zip}")
                
                # Extrair o arquivo ZIP para obter o TXT para processamento
                txt_path = None
                
                # Verificamos se o TXT já existe (caso incomum)
                txt_nome = nome_zip.replace('.ZIP', '.TXT')
                txt_caminho = os.path.join(diretorio, txt_nome)
                
                if os.path.exists(txt_caminho):
                    logger.info(f"Arquivo TXT {txt_nome} já existe")
                    txt_path = txt_caminho
                else:
                    # Extrair o ZIP para obter o TXT
                    try:
                        from fii_utils.zip_utils import extrair_zip
                        logger.info(f"Extraindo ZIP {nome_zip} para processamento")
                        extracted_files = extrair_zip(caminho_completo, diretorio)
                        
                        # Procurar o arquivo TXT extraído
                        for arquivo in extracted_files:
                            if arquivo.upper().endswith('.TXT'):
                                txt_path = arquivo
                                break
                                
                        if not txt_path:
                            logger.error(f"Arquivo TXT não encontrado após extração de {nome_zip}")
                            continue
                    except Exception as e:
                        logger.error(f"Erro ao extrair arquivo ZIP {nome_zip}: {e}")
                        continue
                
                # Criar o objeto ArquivoCotacao com o TXT extraído
                arquivo = ArquivoCotacao(txt_path)
                arquivos_para_processar.append((arquivo, False))
                nomes_processados.add(nome_zip)
            
            elif modificado:
                logger.info(f"Arquivo ZIP modificado: {nome_zip}")
                
                # Processo similar ao caso de arquivo novo
                txt_nome = nome_zip.replace('.ZIP', '.TXT')
                txt_caminho = os.path.join(diretorio, txt_nome)
                
                # Se o TXT existir, remova-o para evitar inconsistências
                if os.path.exists(txt_caminho):
                    try:
                        os.remove(txt_caminho)
                        logger.info(f"Arquivo TXT antigo {txt_nome} removido")
                    except Exception as e:
                        logger.warning(f"Não foi possível remover arquivo TXT antigo {txt_nome}: {e}")
                
                # Extrair o ZIP para obter o TXT atualizado
                try:
                    from fii_utils.zip_utils import extrair_zip
                    logger.info(f"Extraindo ZIP modificado {nome_zip} para processamento")
                    extracted_files = extrair_zip(caminho_completo, diretorio)
                    
                    txt_path = None
                    for arquivo in extracted_files:
                        if arquivo.upper().endswith('.TXT'):
                            txt_path = arquivo
                            break
                            
                    if not txt_path:
                        logger.error(f"Arquivo TXT não encontrado após extração de {nome_zip}")
                        continue
                        
                    arquivo = ArquivoCotacao(txt_path)
                    arquivos_para_processar.append((arquivo, True))
                    nomes_processados.add(nome_zip)
                    
                except Exception as e:
                    logger.error(f"Erro ao extrair arquivo ZIP modificado {nome_zip}: {e}")
                    continue
                
        except ValueError as e:
            logger.warning(f"Arquivo ignorado: {nome_zip}. Erro: {e}")
    
    # Ordena os arquivos: primeiro os anuais, depois os mensais, por fim os diários
    arquivos_para_processar.sort(key=lambda x: (
        0 if x[0].tipo == 'anual' else (1 if x[0].tipo == 'mensal' else 2),
        x[0].data_inicio
    ))
    
    logger.info(f"Encontrados {len(arquivos_para_processar)} arquivos para processamento")
    return arquivos_para_processar


def processar_arquivo(arquivo_cotacao, cotacoes_manager, arquivos_manager, logger, substituir_existentes=False):
    """
    Processa um único arquivo de cotação e o registra no banco de dados.
    
    Args:
        arquivo_cotacao: Objeto ArquivoCotacao a ser processado
        cotacoes_manager: Instância do CotacoesManager
        arquivos_manager: Instância do ArquivosProcessadosManager
        logger: Logger para registro de eventos
        substituir_existentes: Se True, remove registros existentes do período
        
    Returns:
        int: Número de registros inseridos
    """
    logger.info(f"Processando arquivo: {arquivo_cotacao}")
    
    try:
        # Processa o arquivo e insere os registros no banco
        registros = cotacoes_manager.processar_arquivo(
            arquivo_cotacao, 
            substituir_existentes=substituir_existentes
        )
        
        # Se foram inseridos registros, registra o arquivo como processado
        if registros > 0:
            logger.info(f"Arquivo {arquivo_cotacao.nome_arquivo} processado. Registros inseridos: {registros}")
        else:
            logger.warning(f"Nenhum registro inserido para o arquivo {arquivo_cotacao.nome_arquivo}")
        
        return registros
        
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {arquivo_cotacao.caminho}: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return 0