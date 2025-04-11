#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
B3 Downloader integrado ao Sistema de Análise de FIIs.
Responsável por baixar e extrair arquivos de cotações da B3 automaticamente.
"""

import os
import time
import random
import logging
import hashlib
import ssl
import socket
import subprocess
import zipfile
import json
import datetime
import calendar
from urllib.parse import urlparse
from fii_utils.zip_utils import extrair_zip

# Importação do sistema unificado de logging
from fii_utils.logging_manager import get_logger

# Importação dos gerenciadores centralizados
from fii_utils.config_manager import get_config_manager
from fii_utils.calendar_manager import get_calendar_manager

# Enumeração dos tipos de períodos suportados
PERIOD_TYPES = {
    "daily": "D",
    "monthly": "M",
    "yearly": "A"
}

def setup_logging():
    """
    Configura o sistema de logging para o módulo de download.
    Utiliza o sistema unificado de logging e obtém parâmetros da configuração centralizada.
    
    Returns:
        Tuple de loggers (logger, security_logger)
    """
    # Obter configuração do gerenciador centralizado
    config_manager = get_config_manager()
    log_level_str = config_manager.get("log_level", "INFO")
    log_dir = config_manager.get("log_dir")
    
    # Determinar o nível de logging
    log_level = getattr(logging, log_level_str)
    
    # Configurar os loggers usando o sistema unificado
    logger = get_logger(
        'b3_downloader',
        log_dir=log_dir,
        console=True,
        file=True,
        level=log_level
    )
    
    security_logger = get_logger(
        'b3_security',
        log_dir=log_dir,
        console=True,
        file=True,
        level=logging.INFO
    )
    
    return logger, security_logger

def corrigir_permissoes_diretorio(diretorio, permissoes):
    """
    Corrige as permissões de um diretório para um valor mais seguro.
    
    Args:
        diretorio: Caminho do diretório
        permissoes: String com permissões no formato octal (ex: "750")
        
    Returns:
        bool: True se as permissões foram corrigidas, False caso contrário
    """
    logger = get_logger('b3_security')
    
    if not os.path.exists(diretorio):
        return False
    
    if os.name != 'posix':
        logger.warning(f"Correção de permissões não suportada em sistemas não-POSIX")
        return False
    
    try:
        # Converter string de permissões (como "750") para octal
        perm_octal = int(permissoes, 8)
        os.chmod(diretorio, perm_octal)
        logger.info(f"Permissões do diretório {diretorio} corrigidas para {permissoes}")
        return True
    except Exception as e:
        logger.error(f"Erro ao corrigir permissões do diretório {diretorio}: {e}")
        return False

def obter_impressao_digital_certificado(hostname, port=443):
    """
    Obtém a impressão digital SHA-256 do certificado do servidor.
    
    Args:
        hostname: Nome do host
        port: Porta (padrão: 443)
        
    Returns:
        str: Impressão digital do certificado
    """
    context = ssl.create_default_context()
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    
    try:
        with socket.create_connection((hostname, port), timeout=10) as sock:
            with context.wrap_socket(sock, server_hostname=hostname) as ssock:
                cert = ssock.getpeercert(binary_form=True)
                return hashlib.sha256(cert).hexdigest()
    except Exception as e:
        logger = get_logger('b3_downloader')
        logger.error(f"Erro ao obter impressão digital: {e}")
        raise

def registrar_impressao_digital(impressao_digital):
    """
    Registra a impressão digital do certificado para monitoramento.
    
    Args:
        impressao_digital: Impressão digital do certificado
        
    Returns:
        bool: True se o registro foi bem-sucedido
    """
    # Obter loggers
    security_logger = get_logger('b3_security')
    
    # Obter configuração
    config_manager = get_config_manager()
    log_dir = config_manager.get("log_dir")
    
    history_file = os.path.join(log_dir, "fingerprint_history.csv")
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    if not os.path.exists(history_file):
        with open(history_file, 'w') as f:
            f.write("timestamp,fingerprint\n")
    
    with open(history_file, 'a') as f:
        f.write(f"{now},{impressao_digital}\n")
    
    fingerprints = []
    try:
        with open(history_file, 'r') as f:
            next(f)
            for line in f:
                if line.strip():
                    parts = line.strip().split(',')
                    if len(parts) >= 2:
                        fingerprints.append(parts[1])
    except Exception as e:
        security_logger.error(f"Erro ao ler histórico de fingerprints: {e}")
    
    if len(fingerprints) > 1 and fingerprints[-1] != fingerprints[-2]:
        security_logger.warning("ALERTA: Mudança na impressão digital do certificado detectada!")
        security_logger.warning(f"Anterior: {fingerprints[-2]}")
        security_logger.warning(f"Atual: {fingerprints[-1]}")
        
        # Verificar se é uma mudança conhecida (por exemplo, renovação planejada)
        known_transitions_file = os.path.join(log_dir, "known_fingerprint_changes.json")
        if os.path.exists(known_transitions_file):
            try:
                with open(known_transitions_file, 'r') as f:
                    known_transitions = json.load(f)
                
                transition = f"{fingerprints[-2]}:{fingerprints[-1]}"
                if transition in known_transitions:
                    security_logger.info(f"Esta é uma mudança de certificado conhecida: {known_transitions[transition]}")
                    return True
            except Exception as e:
                security_logger.error(f"Erro ao verificar transições conhecidas: {e}")
        
        security_logger.warning("Continuando com alerta de segurança. Verifique manualmente o certificado.")
    
    return True

def verificar_seguranca_ambiente():
    """
    Verifica se o ambiente atende aos requisitos mínimos de segurança.
    
    Returns:
        bool: True se o ambiente atende aos requisitos, False caso contrário
    """
    # Obter loggers
    security_logger = get_logger('b3_security')
    
    # Obter configuração
    config_manager = get_config_manager()
    config = config_manager.get_config()
    
    security_logger.info("=== Verificação de Segurança do Ambiente ===")
    
    verificacoes = []
    problemas_corrigiveis = []
    
    # Verificar se o curl está atualizado
    try:
        result = subprocess.run(['curl', '--version'], capture_output=True, text=True)
        versao = result.stdout.split()[1] if result.stdout else "desconhecida"
        verificacoes.append(("Versão do curl", versao, True))
    except:
        verificacoes.append(("curl instalado", "Não", False))
    
    # Verificar OpenSSL
    try:
        result = subprocess.run(['openssl', 'version'], capture_output=True, text=True)
        versao = result.stdout.split()[1] if result.stdout else "desconhecida"
        verificacoes.append(("Versão do OpenSSL", versao, True))
    except:
        verificacoes.append(("OpenSSL instalado", "Não", False))
    
    # Verificar permissões dos diretórios
    for dir_name, dir_path in [
        ("Certificados", config["cert_dir"]),
        ("Logs", config["log_dir"]),
        ("Dados", config["data_dir"])
    ]:
        os.makedirs(dir_path, exist_ok=True)
        if os.path.exists(dir_path):
            try:
                if os.name == 'posix':
                    mode = oct(os.stat(dir_path).st_mode)[-3:]
                    seguro = int(mode[1]) < 6 and int(mode[2]) < 6
                    verificacoes.append((f"Permissões do diretório {dir_name}", mode, seguro))
                    
                    if not seguro:
                        problemas_corrigiveis.append((dir_path, f"{dir_name} tem permissões inseguras {mode}", 
                                                    f"Ajuste para {config['secure_permissions']} (chmod {config['secure_permissions']} {dir_path})"))
            except Exception as e:
                security_logger.error(f"Erro ao verificar permissões de {dir_path}: {e}")
    
    # Verificar capacidade de criar arquivos nos diretórios
    for dir_name, dir_path in [
        ("Certificados", config["cert_dir"]),
        ("Logs", config["log_dir"]),
        ("Dados", config["data_dir"])
    ]:
        test_file = os.path.join(dir_path, f"test_{int(time.time())}.tmp")
        try:
            with open(test_file, 'w') as f:
                f.write("test")
            os.remove(test_file)
            verificacoes.append((f"Permissão de escrita em {dir_name}", "Sim", True))
        except:
            verificacoes.append((f"Permissão de escrita em {dir_name}", "Não", False))
    
    # Exibir resultados
    for desc, valor, ok in verificacoes:
        status = "✓" if ok else "✗"
        security_logger.info(f"{status} {desc}: {valor}")
    
    # Tentar corrigir problemas se configurado
    if problemas_corrigiveis and config["fix_permissions"]:
        security_logger.info("Tentando corrigir problemas de permissões...")
        for dir_path, problema, solucao in problemas_corrigiveis:
            corrigir_permissoes_diretorio(dir_path, config["secure_permissions"])
    
    # Registrar problemas e sugestões
    problemas_criticos = [desc for desc, _, ok in verificacoes if not ok]
    if problemas_criticos:
        security_logger.error(f"Problemas críticos encontrados: {', '.join(problemas_criticos)}")
    
    if problemas_corrigiveis and not config["fix_permissions"]:
        security_logger.warning("Problemas corrigíveis encontrados:")
        for _, problema, solucao in problemas_corrigiveis:
            security_logger.warning(f"  - {problema}")
            security_logger.warning(f"    Solução: {solucao}")
        security_logger.warning("Para corrigir automaticamente, execute com o parâmetro --fix-permissions ou")
        security_logger.warning("defina 'fix_permissions': true no arquivo config.json")
    
    return all(ok for _, _, ok in verificacoes)

def limpar_certificados_antigos():
    """
    Remove certificados mais antigos que X dias.
    """
    # Obter loggers
    security_logger = get_logger('b3_security')
    
    # Obter configuração
    config_manager = get_config_manager()
    cert_dir = config_manager.get("cert_dir")
    dias = config_manager.get("cert_rotation_days", 7)
    
    if not os.path.exists(cert_dir):
        return
    
    now = time.time()
    count = 0
    
    security_logger.info(f"Iniciando limpeza de certificados com mais de {dias} dias...")
    
    for arquivo in os.listdir(cert_dir):
        if arquivo.startswith("b3_cert_") and arquivo.endswith(".pem"):
            caminho = os.path.join(cert_dir, arquivo)
            if os.path.isfile(caminho):
                if os.stat(caminho).st_mtime < now - dias * 86400:
                    try:
                        os.remove(caminho)
                        count += 1
                    except Exception as e:
                        logger = get_logger('b3_downloader')
                        logger.error(f"Erro ao remover certificado antigo {arquivo}: {e}")
    
    security_logger.info(f"Limpeza concluída. {count} certificados antigos removidos.")

def verificar_arquivo_existe(url):
    """
    Verifica se um arquivo existe no servidor.
    
    Args:
        url: URL do arquivo
        
    Returns:
        bool: True se o arquivo existe, False caso contrário
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    # Obter configuração
    config_manager = get_config_manager()
    user_agent = config_manager.get("user_agent")
    
    try:
        # Comando curl para verificar se o arquivo existe
        curl_command = [
            'curl',
            '--head',  # Apenas cabeçalhos HTTP
            '-L',      # Seguir redirecionamentos
            '-k',      # Ignorar verificação SSL
            '-A', user_agent,
            url
        ]
        
        logger.debug(f"Verificando existência de {url}")
        process = subprocess.run(curl_command, timeout=10, capture_output=True, text=True)
        
        # Procurar por código de status HTTP
        if "HTTP/" in process.stdout:
            status_line = [line for line in process.stdout.split('\n') if line.startswith("HTTP/")][-1]
            status_code = int(status_line.split()[1]) if status_line else 0
            logger.debug(f"Código de status HTTP: {status_code}")
            
            # 200 OK = arquivo existe, 404 Not Found = arquivo não existe
            if status_code == 200:
                logger.info(f"Arquivo encontrado: {url}")
                return True
            elif status_code == 404:
                logger.info(f"Arquivo não encontrado: {url}")
                return False
            else:
                logger.warning(f"Código de status HTTP inesperado: {status_code}")
                return False
        else:
            logger.warning("Não foi possível determinar o status HTTP")
            return False
    except Exception as e:
        logger.error(f"Erro ao verificar existência do arquivo: {e}")
        return False

def gerar_nome_arquivo(periodo, dia=None, mes=None, ano=None):
    """
    Gera o nome do arquivo de acordo com o período.
    
    Args:
        periodo: Tipo de período ('daily', 'monthly', 'yearly')
        dia: Dia (string de 2 dígitos, apenas para período diário)
        mes: Mês (string de 2 dígitos, para períodos diário e mensal)
        ano: Ano (string de 4 dígitos)
        
    Returns:
        string: Nome do arquivo no formato esperado pela B3
    """
    periodo_codigo = PERIOD_TYPES.get(periodo)
    
    if not periodo_codigo:
        raise ValueError(f"Período inválido: {periodo}")
    
    if periodo == "daily" and (not dia or not mes or not ano):
        raise ValueError("Dia, mês e ano são obrigatórios para arquivos diários")
    
    if periodo == "monthly" and (not mes or not ano):
        raise ValueError("Mês e ano são obrigatórios para arquivos mensais")
    
    if periodo == "yearly" and not ano:
        raise ValueError("Ano é obrigatório para arquivos anuais")
    
    if periodo == "daily":
        return f"COTAHIST_D{dia}{mes}{ano}.ZIP"
    elif periodo == "monthly":
        return f"COTAHIST_M{mes}{ano}.ZIP"
    else:  # yearly
        return f"COTAHIST_A{ano}.ZIP"

def verificar_arquivo_disponivel(periodo, dia, mes, ano):
    """
    Verifica se um arquivo específico está disponível para download.
    
    Args:
        periodo: Tipo de período ('daily', 'monthly', 'yearly')
        dia: Dia (string de 2 dígitos)
        mes: Mês (string de 2 dígitos)
        ano: Ano (string de 4 dígitos)
        
    Returns:
        tuple: (bool, string) - (disponível, nome do arquivo)
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    # Obter configuração
    config_manager = get_config_manager()
    base_url = config_manager.get("base_url")
    
    try:
        # Gerar nome do arquivo
        filename = gerar_nome_arquivo(periodo, dia, mes, ano)
        url = f"{base_url}{filename}"
        
        # Verificar se o arquivo existe
        disponivel = verificar_arquivo_existe(url)
        
        return disponivel, filename
    except Exception as e:
        logger.error(f"Erro ao verificar disponibilidade do arquivo: {e}")
        return False, None

def baixar_certificado(hostname, cert_path):
    """
    Baixa o certificado SSL do servidor com tratamento de espaços no caminho.
    
    Args:
        hostname: Nome do host
        cert_path: Caminho para salvar o certificado
        
    Returns:
        bool: True se o certificado foi baixado com sucesso, False caso contrário
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    try:
        # Criar diretório se não existir
        os.makedirs(os.path.dirname(cert_path), exist_ok=True)
        
        # Em ambientes WSL, os caminhos com espaços podem causar problemas
        # Vamos usar um caminho temporário sem espaços, e depois copiar
        temp_cert_path = f"/tmp/b3_cert_{int(time.time())}.pem"
        
        openssl_cmd = f"openssl s_client -showcerts -connect {hostname}:443 </dev/null 2>/dev/null | openssl x509 -outform PEM > {temp_cert_path}"
        logger.debug(f"Executando comando OpenSSL: {openssl_cmd}")
        subprocess.run(openssl_cmd, shell=True, check=True)
        
        # Verificar se o certificado foi criado
        if os.path.exists(temp_cert_path) and os.path.getsize(temp_cert_path) > 0:
            # Copiar para o destino final
            with open(temp_cert_path, 'rb') as src, open(cert_path, 'wb') as dst:
                dst.write(src.read())
                
            # Remover arquivo temporário
            os.remove(temp_cert_path)
            
            logger.info(f"Certificado salvo em: {cert_path}")
            return True
        else:
            logger.error("Falha ao gerar certificado SSL")
            return False
    except Exception as e:
        logger.error(f"Erro ao baixar certificado: {e}")
        return False

def baixar_arquivo_b3(filename, output_path, impressao_digital=None):
    """
    Baixa arquivo da B3 usando curl com verificação de impressão digital.
    
    Args:
        filename: Nome do arquivo para baixar
        output_path: Caminho onde salvar o arquivo
        impressao_digital: Impressão digital do certificado esperado (opcional)
        
    Returns:
        bool: True se o download foi bem-sucedido, False caso contrário
    """
    # Obter loggers
    logger = get_logger('b3_downloader')
    security_logger = get_logger('b3_security')
    
    # Obter configuração
    config_manager = get_config_manager()
    config = config_manager.get_config()
    
    base_url = config["base_url"]
    cert_dir = config["cert_dir"]
    
    url = f"{base_url}{filename}"
    hostname = urlparse(base_url).netloc
    
    # Se não foi fornecida uma impressão digital, vamos obtê-la agora
    if impressao_digital is None:
        try:
            impressao_digital = obter_impressao_digital_certificado(hostname)
            registrar_impressao_digital(impressao_digital)
            logger.info(f"Impressão digital obtida: {impressao_digital}")
        except Exception as e:
            logger.error(f"Erro ao obter impressão digital: {e}")
            logger.warning("Continuando sem verificação de impressão digital...")
    
    # Verificar se a impressão digital atual corresponde à esperada
    if impressao_digital:
        try:
            impressao_digital_atual = obter_impressao_digital_certificado(hostname)
            
            if impressao_digital_atual != impressao_digital:
                security_logger.error(f"ALERTA DE SEGURANÇA: A impressão digital do certificado mudou!")
                security_logger.error(f"Esperada: {impressao_digital}")
                security_logger.error(f"Atual: {impressao_digital_atual}")
                
                # Registrar a mudança de impressão digital
                registrar_impressao_digital(impressao_digital_atual)
                security_logger.warning("Continuando com alerta de segurança. Verifique manualmente o certificado.")
                
                # Atualizar a impressão digital
                impressao_digital = impressao_digital_atual
        except Exception as e:
            logger.error(f"Erro ao verificar impressão digital atual: {e}")
    
    # Criar diretório para certificados
    os.makedirs(cert_dir, exist_ok=True)
    
    # Nome do arquivo de certificado baseado no timestamp
    timestamp = int(time.time())
    cert_path = os.path.join(cert_dir, f"b3_cert_{timestamp}.pem")
    
    # Baixar certificado do servidor
    cert_baixado = baixar_certificado(hostname, cert_path)
    
    # Pequeno atraso antes do download
    time.sleep(random.uniform(1.0, 3.0))
    
    # Construir comando curl
    curl_command = [
        'curl',
        '--retry', str(config["max_retries"]),
        '--retry-delay', '2',
        '-L',
        '-A', config["user_agent"],
        '-H', 'Accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        '-H', 'Accept-Language: pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        '-H', f'Referer: https://{hostname}/',
        url,
        '-o', output_path
    ]
    
    # Adicionar verbose se configurado
    if config["enable_curl_verbose"]:
        curl_command.append('-v')
    
    # Verificar se temos um certificado válido
    if cert_baixado and os.path.exists(cert_path) and os.path.getsize(cert_path) > 100:
        curl_command.extend(['--cacert', cert_path])
        logger.info("Usando certificado baixado para verificação SSL")
    else:
        logger.warning("Usando curl com -k (inseguro) porque não conseguimos obter um certificado válido")
        curl_command.append('-k')
    
    # Executar curl
    try:
        logger.info(f"Baixando {filename}...")
        logger.debug(f"Comando curl: {' '.join(curl_command)}")
        
        process = subprocess.run(curl_command, timeout=60, capture_output=True)
        
        if process.returncode == 0:
            logger.info(f"Download de {filename} concluído com sucesso")
            
            # Verificações adicionais no arquivo baixado
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                logger.info(f"Tamanho do arquivo baixado: {file_size} bytes")
                
                if file_size < 100:
                    logger.warning(f"ALERTA: Arquivo baixado é muito pequeno ({file_size} bytes). Verifique o conteúdo.")
                
                # Verificar se é um arquivo ZIP válido
                if output_path.lower().endswith('.zip'):
                    try:
                        with zipfile.ZipFile(output_path, 'r') as zip_ref:
                            file_list = zip_ref.namelist()
                            logger.info(f"Arquivos no ZIP: {', '.join(file_list)}")
                            if not file_list:
                                logger.warning("O arquivo ZIP está vazio!")
                                return False
                    except zipfile.BadZipFile:
                        logger.error("O arquivo baixado não é um ZIP válido!")
                        return False
                    except Exception as e:
                        logger.error(f"Erro ao verificar o arquivo ZIP: {e}")
                        return False
            
            return True
        else:
            logger.error(f"Curl falhou com código: {process.returncode}")
            if process.stderr:
                logger.error(f"Erro: {process.stderr.decode()}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("Timeout ao executar curl.")
        return False
    except Exception as e:
        logger.error(f"Erro ao usar curl: {e}")
        return False

def baixar_com_fallback(dia, mes, ano, force=False):
    """
    Tenta baixar um arquivo com suporte a diferentes formatos.
    Primeiro tenta baixar o arquivo diário, depois o mensal e finalmente o anual.
    
    Args:
        dia: Dia (string de 2 dígitos)
        mes: Mês (string de 2 dígitos)
        ano: Ano (string de 4 dígitos)
        force: Se deve forçar o download mesmo se o arquivo já existir
        
    Returns:
        tuple: (status, zip_path, txt_path)
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    # Obter configuração
    config_manager = get_config_manager()
    data_dir = config_manager.get("data_dir")
    
    # Primeiro tenta baixar arquivo diário
    try:
        filename_daily = gerar_nome_arquivo("daily", dia, mes, ano)
        zip_path = os.path.join(data_dir, filename_daily)
        txt_path = zip_path.replace('.ZIP', '.TXT')
        
        # Verificar se o arquivo já existe e se não estamos forçando o download
        if os.path.exists(txt_path) and not force:
            logger.info(f"Arquivo diário {filename_daily} já existe. Use --force para baixar novamente.")
            return "exists", zip_path, txt_path
        
        # Verificar se arquivo está disponível
        disponivel, _ = verificar_arquivo_disponivel("daily", dia, mes, ano)
        
        if disponivel:
            # Baixar arquivo
            if baixar_arquivo_b3(filename_daily, zip_path):
                # Extrair arquivo
                extracted_files = extrair_zip(zip_path, data_dir)
                if extracted_files:
                    # Encontrar o arquivo TXT
                    for ext_file in extracted_files:
                        if ext_file.upper().endswith('.TXT'):
                            txt_path = ext_file
                            break
                    
                    logger.info(f"Arquivo diário {filename_daily} baixado e extraído com sucesso")
                    return "success", zip_path, txt_path
                else:
                    logger.error(f"Falha ao extrair arquivo diário {filename_daily}")
                    return "extract_error", zip_path, None
            else:
                logger.error(f"Falha ao baixar arquivo diário {filename_daily}")
        else:
            logger.info(f"Arquivo diário {filename_daily} não disponível")
            
            # Se o arquivo diário não está disponível, tenta baixar o mensal
            try:
                filename_monthly = gerar_nome_arquivo("monthly", None, mes, ano)
                zip_path = os.path.join(data_dir, filename_monthly)
                txt_path = zip_path.replace('.ZIP', '.TXT')
                
                # Verificar se o arquivo já existe e se não estamos forçando o download
                if os.path.exists(txt_path) and not force:
                    logger.info(f"Arquivo mensal {filename_monthly} já existe. Use --force para baixar novamente.")
                    return "exists", zip_path, txt_path
                
                # Verificar se arquivo está disponível
                disponivel, _ = verificar_arquivo_disponivel("monthly", None, mes, ano)
                
                if disponivel:
                    # Baixar arquivo
                    if baixar_arquivo_b3(filename_monthly, zip_path):
                        # Extrair arquivo
                        extracted_files = extrair_zip(zip_path, data_dir)
                        if extracted_files:
                            # Encontrar o arquivo TXT
                            for ext_file in extracted_files:
                                if ext_file.upper().endswith('.TXT'):
                                    txt_path = ext_file
                                    break
                            
                            logger.info(f"Arquivo mensal {filename_monthly} baixado e extraído com sucesso")
                            return "success", zip_path, txt_path
                        else:
                            logger.error(f"Falha ao extrair arquivo mensal {filename_monthly}")
                            return "extract_error", zip_path, None
                    else:
                        logger.error(f"Falha ao baixar arquivo mensal {filename_monthly}")
                else:
                    logger.info(f"Arquivo mensal {filename_monthly} não disponível")
                    
                    # Se o arquivo mensal não está disponível, tenta baixar o anual
                    try:
                        filename_yearly = gerar_nome_arquivo("yearly", None, None, ano)
                        zip_path = os.path.join(data_dir, filename_yearly)
                        txt_path = zip_path.replace('.ZIP', '.TXT')
                        
                        # Verificar se o arquivo já existe e se não estamos forçando o download
                        if os.path.exists(txt_path) and not force:
                            logger.info(f"Arquivo anual {filename_yearly} já existe. Use --force para baixar novamente.")
                            return "exists", zip_path, txt_path
                        
                        # Verificar se arquivo está disponível
                        disponivel, _ = verificar_arquivo_disponivel("yearly", None, None, ano)
                        
                        if disponivel:
                            # Baixar arquivo
                            if baixar_arquivo_b3(filename_yearly, zip_path):
                                # Extrair arquivo
                                extracted_files = extrair_zip(zip_path, data_dir)
                                if extracted_files:
                                    # Encontrar o arquivo TXT
                                    for ext_file in extracted_files:
                                        if ext_file.upper().endswith('.TXT'):
                                            txt_path = ext_file
                                            break
                                    
                                    logger.info(f"Arquivo anual {filename_yearly} baixado e extraído com sucesso")
                                    return "success", zip_path, txt_path
                                else:
                                    logger.error(f"Falha ao extrair arquivo anual {filename_yearly}")
                                    return "extract_error", zip_path, None
                            else:
                                logger.error(f"Falha ao baixar arquivo anual {filename_yearly}")
                        else:
                            logger.info(f"Arquivo anual {filename_yearly} não disponível")
                    except Exception as e:
                        logger.error(f"Erro ao tentar baixar arquivo anual: {e}")
            except Exception as e:
                logger.error(f"Erro ao tentar baixar arquivo mensal: {e}")
    except Exception as e:
        logger.error(f"Erro ao tentar baixar arquivo: {e}")
    
    return "not_available", None, None

def determinar_arquivos_para_baixar(arquivos_manager):
    """
    Determina quais arquivos precisam ser baixados com base no último arquivo processado.
    
    Args:
        arquivos_manager: Instância do ArquivosProcessadosManager
        
    Returns:
        list: Lista de tuplas (dia, mes, ano) para baixar
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    # Obter configuração
    config_manager = get_config_manager()
    
    # Obter gerenciador de calendário
    calendar_manager = get_calendar_manager()
    
    try:
        # Listar arquivos já processados para encontrar o mais recente
        arquivos = arquivos_manager.listar_arquivos_processados()
        
        # Filtrar apenas arquivos diários
        arquivos_diarios = [a for a in arquivos if a['tipo'] == 'diario']
        
        if not arquivos_diarios:
            logger.warning("Nenhum arquivo diário encontrado no banco. Considerando baixar apenas o dia atual ou anterior.")
            
            # Determinar data atual
            hoje = datetime.datetime.now().date()
            
            # Verificar se a data atual é dia útil
            if calendar_manager.is_trading_day(hoje):
                # Verificar se o arquivo já está disponível (nem sempre estará no mesmo dia)
                dia = hoje.strftime('%d')
                mes = hoje.strftime('%m')
                ano = hoje.strftime('%Y')
                
                disponivel, _ = verificar_arquivo_disponivel("daily", dia, mes, ano)
                
                if disponivel:
                    logger.info(f"Arquivo diário para {dia}/{mes}/{ano} disponível para download")
                    return [(dia, mes, ano)]
                else:
                    logger.info(f"Arquivo diário para {dia}/{mes}/{ano} ainda não disponível")
            
            # Tentar o dia útil anterior
            dia_anterior = calendar_manager.get_previous_trading_day(hoje)
            dia = dia_anterior.strftime('%d')
            mes = dia_anterior.strftime('%m')
            ano = dia_anterior.strftime('%Y')
            
            logger.info(f"Tentando baixar o dia útil anterior: {dia}/{mes}/{ano}")
            return [(dia, mes, ano)]
        
        # Ordenar por data (assumindo nome no formato COTAHIST_D[dia][mes][ano].ZIP)
        # Extrair a data do nome do arquivo e converter para objeto date
        datas = []
        for arquivo in arquivos_diarios:
            nome = arquivo['nome_arquivo']
            if nome.startswith('COTAHIST_D') and nome.endswith('.ZIP'):
                try:
                    # Extrair dia, mês e ano do nome
                    # COTAHIST_DDDMMAAAA.ZIP
                    #           ^^^^^^^^
                    data_str = nome[10:18]  # Posições 10-17 (dia, mês, ano)
                    dia = data_str[0:2]
                    mes = data_str[2:4]
                    ano = data_str[4:8]
                    
                    data = datetime.date(int(ano), int(mes), int(dia))
                    datas.append((data, dia, mes, ano))
                except (ValueError, IndexError) as e:
                    logger.warning(f"Nome de arquivo {nome} não está no formato esperado: {e}")
        
        if not datas:
            logger.warning("Não foi possível extrair datas dos nomes dos arquivos")
            return []
        
        # Ordenar por data
        datas.sort(key=lambda x: x[0])
        
        # Obter a data mais recente
        ultima_data, _, _, _ = datas[-1]
        
        # Determinar datas a baixar (dias úteis entre a última data e hoje)
        hoje = datetime.datetime.now().date()
        
        # Ajustar última data para o dia seguinte
        proxima_data = ultima_data + datetime.timedelta(days=1)
        
        # Lista de datas para baixar
        datas_para_baixar = []
        
        # Percorrer datas de proxima_data até hoje
        data_atual = proxima_data
        while data_atual <= hoje:
            # Verificar se é dia útil na B3
            if calendar_manager.is_trading_day(data_atual):
                dia = data_atual.strftime('%d')
                mes = data_atual.strftime('%m')
                ano = data_atual.strftime('%Y')
                
                # Verificar se o arquivo está disponível
                disponivel, _ = verificar_arquivo_disponivel("daily", dia, mes, ano)
                
                if disponivel:
                    logger.info(f"Arquivo diário para {dia}/{mes}/{ano} disponível para download")
                    datas_para_baixar.append((dia, mes, ano))
                else:
                    logger.info(f"Arquivo diário para {dia}/{mes}/{ano} ainda não disponível")
            
            # Avançar para o próximo dia
            data_atual += datetime.timedelta(days=1)
        
        return datas_para_baixar
    
    except Exception as e:
        logger.error(f"Erro ao determinar arquivos para baixar: {e}")
        return []

def baixar_multiplos_arquivos(datas, force=False):
    """
    Baixa múltiplos arquivos para as datas especificadas.
    
    Args:
        datas: Lista de tuplas (dia, mes, ano)
        force: Se deve forçar o download mesmo se o arquivo já existir
        
    Returns:
        tuple: (sucessos, falhas, nao_disponiveis, arquivos_txt)
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    # Obter configuração
    config_manager = get_config_manager()
    
    # Estatísticas
    sucessos = 0
    falhas = 0
    nao_disponiveis = 0
    arquivos_txt = []
    
    # Obter intervalos de espera
    wait_min, wait_max = config_manager.get("wait_between_downloads", [3.0, 7.0])
    
    for i, (dia, mes, ano) in enumerate(datas):
        logger.info(f"Baixando arquivo {i+1}/{len(datas)}: {dia}/{mes}/{ano}")
        
        status, zip_path, txt_path = baixar_com_fallback(dia, mes, ano, force)
        
        if status == "success":
            sucessos += 1
            if txt_path:
                arquivos_txt.append(txt_path)
                logger.info(f"Download completo: {dia}/{mes}/{ano} -> {txt_path}")
            else:
                logger.warning(f"Download completo, mas arquivo TXT não encontrado: {dia}/{mes}/{ano}")
        elif status == "exists":
            sucessos += 1
            if txt_path:
                arquivos_txt.append(txt_path)
                logger.info(f"Arquivo já existe: {dia}/{mes}/{ano} -> {txt_path}")
            else:
                logger.warning(f"Arquivo marcado como existente, mas TXT não encontrado: {dia}/{mes}/{ano}")
        elif status == "extract_error":
            falhas += 1
            logger.error(f"Falha ao extrair arquivo: {dia}/{mes}/{ano}")
        elif status == "not_available":
            nao_disponiveis += 1
            logger.warning(f"Arquivo não disponível: {dia}/{mes}/{ano}")
        else:
            falhas += 1
            logger.error(f"Status desconhecido: {status} para {dia}/{mes}/{ano}")
        
        # Esperar entre downloads (exceto no último)
        if i < len(datas) - 1:
            wait_time = random.uniform(wait_min, wait_max)
            logger.debug(f"Aguardando {wait_time:.2f} segundos antes do próximo download...")
            time.sleep(wait_time)
    
    # Resumo final
    logger.info(f"Resumo do download: {sucessos} sucessos, {falhas} falhas, {nao_disponiveis} não disponíveis")
    logger.info(f"Arquivos TXT disponíveis: {len(arquivos_txt)}")
    
    return sucessos, falhas, nao_disponiveis, arquivos_txt

def baixar_arquivos_diarios(data_inicio, data_fim, force=False):
    """
    Baixa arquivos diários em um período.
    
    Args:
        data_inicio: Data inicial (datetime.datetime)
        data_fim: Data final (datetime.datetime)
        force: Se deve forçar o download mesmo se o arquivo já existir
        
    Returns:
        list: Lista de tuplas (dia, mes, ano) baixados com sucesso
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    # Obter configuração
    config_manager = get_config_manager()
    
    # Obter gerenciador de calendário
    calendar_manager = get_calendar_manager()
    
    # Verifica se as datas são objetos datetime
    if not isinstance(data_inicio, datetime.datetime):
        data_inicio = datetime.datetime.combine(data_inicio, datetime.datetime.min.time())
    if not isinstance(data_fim, datetime.datetime):
        data_fim = datetime.datetime.combine(data_fim, datetime.datetime.min.time())
    
    logger.info(f"Preparando download de arquivos diários de {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}")
    
    # Lista de datas (dia, mes, ano) para baixar
    datas = []
    
    # Percorrer datas
    data_atual = data_inicio
    while data_atual <= data_fim:
        # Verificar se é dia útil na B3
        if calendar_manager.is_trading_day(data_atual):
            dia = data_atual.strftime('%d')
            mes = data_atual.strftime('%m')
            ano = data_atual.strftime('%Y')
            datas.append((dia, mes, ano))
        
        # Avançar para o próximo dia
        data_atual += datetime.timedelta(days=1)
    
    logger.info(f"Serão baixados até {len(datas)} arquivos diários")
    
    # Baixar arquivos
    sucessos, falhas, nao_disponiveis, arquivos_txt = baixar_multiplos_arquivos(datas, force)
    
    return datas

def baixar_arquivos_mensais(mes_inicio, ano_inicio, mes_fim, ano_fim, force=False):
    """
    Baixa arquivos mensais em um período.
    
    Args:
        mes_inicio: Mês inicial (int)
        ano_inicio: Ano inicial (int)
        mes_fim: Mês final (int)
        ano_fim: Ano final (int)
        force: Se deve forçar o download mesmo se o arquivo já existir
        
    Returns:
        list: Lista de tuplas (mes, ano) baixados com sucesso
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    logger.info(f"Preparando download de arquivos mensais de {mes_inicio:02d}/{ano_inicio} a {mes_fim:02d}/{ano_fim}")
    
    # Lista de meses (mes, ano) para baixar
    meses = []
    
    # Percorrer meses
    ano_atual = ano_inicio
    mes_atual = mes_inicio
    
    while ano_atual < ano_fim or (ano_atual == ano_fim and mes_atual <= mes_fim):
        meses.append((f"{mes_atual:02d}", f"{ano_atual}"))
        
        # Avançar para o próximo mês
        mes_atual += 1
        if mes_atual > 12:
            mes_atual = 1
            ano_atual += 1
    
    logger.info(f"Serão baixados até {len(meses)} arquivos mensais")
    
    # Sucessos (lista de tuplas mes, ano)
    sucessos_meses = []
    
    # Baixar arquivos mês a mês
    for mes, ano in meses:
        # Obter o último dia do mês para verificar
        ultimo_dia = str(calendar.monthrange(int(ano), int(mes))[1]).zfill(2)
        
        disponivel, _ = verificar_arquivo_disponivel("monthly", None, mes, ano)
        
        if disponivel:
            # Baixar o arquivo
            status, zip_path, txt_path = baixar_com_fallback(ultimo_dia, mes, ano, force)
            
            if status in ["success", "exists"]:
                sucessos_meses.append((mes, ano))
            else:
                logger.error(f"Falha ao baixar arquivo mensal para {mes}/{ano}")
        else:
            logger.warning(f"Arquivo mensal para {mes}/{ano} não disponível")
    
    return sucessos_meses

def baixar_arquivos_anuais(ano_inicio, ano_fim, force=False):
    """
    Baixa arquivos anuais em um período.
    
    Args:
        ano_inicio: Ano inicial (int)
        ano_fim: Ano final (int)
        force: Se deve forçar o download mesmo se o arquivo já existir
        
    Returns:
        list: Lista de anos baixados com sucesso
    """
    # Obter logger
    logger = get_logger('b3_downloader')
    
    logger.info(f"Preparando download de arquivos anuais de {ano_inicio} a {ano_fim}")
    
    # Lista de anos para baixar
    anos = list(range(ano_inicio, ano_fim + 1))
    
    logger.info(f"Serão baixados até {len(anos)} arquivos anuais")
    
    # Sucessos (lista de anos)
    sucessos_anos = []
    
    # Baixar arquivos ano a ano
    for ano in anos:
        ano_str = str(ano)
        
        disponivel, _ = verificar_arquivo_disponivel("yearly", None, None, ano_str)
        
        if disponivel:
            # Baixar o arquivo
            status, zip_path, txt_path = baixar_com_fallback("31", "12", ano_str, force)
            
            if status in ["success", "exists"]:
                sucessos_anos.append(ano_str)
            else:
                logger.error(f"Falha ao baixar arquivo anual para {ano}")
        else:
            logger.warning(f"Arquivo anual para {ano} não disponível")
    
    return sucessos_anos

def inicializar():
    """
    Inicializa o módulo de download, configurando loggers e garantindo diretórios.
    
    Returns:
        ConfigManager: Instância do gerenciador de configuração
    """
    # Obter gerenciador de configuração
    config_manager = get_config_manager()
    
    # Configurar o logging
    logger, security_logger = setup_logging()
    
    # Garantir que os diretórios necessários existam
    config_manager.ensure_directories()
    
    return config_manager