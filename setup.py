#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de configuração inicial para o Sistema de Análise de FIIs.
Cria a estrutura de diretórios e instala as dependências necessárias.
"""

import os
import sys
import subprocess
import json

# Diretórios a serem criados
DIRECTORIES = [
    "historico_cotacoes",
    "logs",
    "config/certificates"
]

# Arquivo de configuração padrão
DEFAULT_CONFIG = {
    "base_url": "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/",
    "data_dir": "historico_cotacoes",
    "cert_dir": "config/certificates",
    "log_dir": "logs",
    "max_retries": 3,
    "backoff_factor": 1.5,
    "wait_between_downloads": [3.0, 7.0],
    "cert_rotation_days": 7,
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "log_level": "INFO",
    "max_log_size_mb": 10,
    "max_log_backups": 5,
    "verify_downloads": True,
    "concurrent_downloads": 1,
    "enable_curl_verbose": True,
    "fix_permissions": False,
    "secure_permissions": "750",
    "default_period": "daily",
    "try_previous_day": True,
    "calendar_cache_days": 30
}

def criar_diretorios():
    """Cria a estrutura de diretórios necessária."""
    print("\nCriando estrutura de diretórios...")
    
    for diretorio in DIRECTORIES:
        caminho = os.path.join(os.path.dirname(os.path.abspath(__file__)), diretorio)
        if not os.path.exists(caminho):
            try:
                os.makedirs(caminho)
                print(f"✓ Diretório criado: {diretorio}")
            except Exception as e:
                print(f"✗ Erro ao criar diretório {diretorio}: {e}")
        else:
            print(f"ℹ Diretório já existe: {diretorio}")

def criar_config():
    """Cria o arquivo de configuração padrão se não existir."""
    print("\nVerificando arquivo de configuração...")
    
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config", "config.json")
    
    if not os.path.exists(config_path):
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(DEFAULT_CONFIG, f, indent=4)
            print(f"✓ Arquivo de configuração criado: config/config.json")
        except Exception as e:
            print(f"✗ Erro ao criar arquivo de configuração: {e}")
    else:
        print(f"ℹ Arquivo de configuração já existe: config/config.json")
        
        # Atualizar config existente removendo holidays se existir
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            
            if 'holidays' in config:
                print("ℹ Removendo lista de feriados obsoleta do arquivo de configuração...")
                config.pop('holidays')
                
                # Adicionar nova configuração se não existir
                if 'calendar_cache_days' not in config:
                    config['calendar_cache_days'] = 30
                    print("ℹ Adicionando nova configuração para cache do calendário...")
                
                with open(config_path, 'w', encoding='utf-8') as f:
                    json.dump(config, f, indent=4)
                print("✓ Arquivo de configuração atualizado com sucesso")
        except Exception as e:
            print(f"✗ Erro ao atualizar arquivo de configuração: {e}")

def instalar_dependencias():
    """Instala as dependências necessárias usando pip."""
    print("\nInstalando dependências...")
    
    requirements = [
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "openpyxl>=3.0.7",
        "pandas_market_calendars>=4.1.4"
    ]
    
    requirements_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), "requirements.txt")
    
    # Cria o arquivo requirements.txt se não existir
    if not os.path.exists(requirements_file):
        try:
            with open(requirements_file, 'w') as f:
                f.write("\n".join(requirements))
            print(f"✓ Arquivo requirements.txt criado")
        except Exception as e:
            print(f"✗ Erro ao criar arquivo requirements.txt: {e}")
    
    # Instala as dependências
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", requirements_file])
        print("✓ Dependências instaladas com sucesso")
    except Exception as e:
        print(f"✗ Erro ao instalar dependências: {e}")
        print("  Tente instalar manualmente com: pip install -r requirements.txt")

def verificar_sistema():
    """Verifica se o sistema atende aos requisitos."""
    print("\nVerificando requisitos de sistema...")
    
    # Verificar Python
    python_version = sys.version_info
    python_ok = python_version.major == 3 and python_version.minor >= 6
    print(f"{'✓' if python_ok else '✗'} Python 3.6+: v{python_version.major}.{python_version.minor}.{python_version.micro}")
    
    # Verificar curl
    try:
        subprocess.check_output(["curl", "--version"])
        curl_ok = True
        print("✓ curl: Instalado")
    except:
        curl_ok = False
        print("✗ curl: Não encontrado")
    
    # Verificar OpenSSL
    try:
        subprocess.check_output(["openssl", "version"])
        openssl_ok = True
        print("✓ OpenSSL: Instalado")
    except:
        openssl_ok = False
        print("✗ OpenSSL: Não encontrado")
    
    if not (python_ok and curl_ok and openssl_ok):
        print("\n⚠ Atenção: Alguns requisitos de sistema não foram encontrados.")
        
        if not curl_ok:
            print("  - curl é necessário para download de arquivos da B3")
            print("    - Ubuntu/Debian: sudo apt install curl")
            print("    - Fedora/CentOS: sudo dnf install curl")
            print("    - macOS: brew install curl")
            print("    - Windows: Instale Git for Windows ou use WSL")
        
        if not openssl_ok:
            print("  - OpenSSL é necessário para verificação de certificados SSL")
            print("    - Ubuntu/Debian: sudo apt install openssl")
            print("    - Fedora/CentOS: sudo dnf install openssl")
            print("    - macOS: brew install openssl")
            print("    - Windows: Instale Git for Windows ou use WSL")

def main():
    """Função principal."""
    print("=== Configuração Inicial do Sistema de Análise de FIIs ===")
    
    verificar_sistema()
    criar_diretorios()
    criar_config()
    instalar_dependencias()
    
    print("\n✅ Configuração inicial concluída!")
    print("\nPróximos passos:")
    print("1. Crie um arquivo eventos.json para gerenciar eventos corporativos")
    print("2. Crie um arquivo fundos.json para exportar cotações de FIIs específicos")
    print("3. Execute o sistema com: python main.py")
    print("\nPara mais informações, consulte o arquivo README.md")

if __name__ == "__main__":
    main()
