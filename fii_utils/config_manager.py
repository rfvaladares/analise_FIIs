"""
Gerenciador centralizado de configuração para o Sistema de Análise de FIIs.
Implementa o padrão Singleton para garantir uma única fonte de configuração em toda a aplicação.
"""

import os
import json
from typing import Dict, Any

from fii_utils.logging_manager import get_logger

class ConfigManager:
    """
    Gerenciador centralizado de configuração que implementa o padrão Singleton.
    Gerencia o carregamento, acesso e atualização das configurações do sistema.
    """
    
    # Variável de classe para armazenar a instância única (Singleton)
    _instance = None
    
    # Definição padrão de configuração
    DEFAULT_CONFIG = {
        "base_url": "https://bvmf.bmfbovespa.com.br/InstDados/SerHist/",
        "data_dir": "historico_cotacoes",  # Será atualizado para caminho absoluto na inicialização
        "cert_dir": "config/certificates",  # Será atualizado para caminho absoluto na inicialização
        "log_dir": "logs",                  # Será atualizado para caminho absoluto na inicialização
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
        "calendar_cache_days": 30,    # Dias para manter o cache do calendário da B3
        "extract_retries": 3,         # Número de tentativas para extrair um arquivo ZIP
        "extract_retry_delay": 2.0    # Tempo de espera (segundos) entre tentativas de extração
    }
    
    def __new__(cls) -> 'ConfigManager':
        """
        Implementação do padrão Singleton. Garante que apenas uma instância da classe seja criada.
        
        Returns:
            Instância única do ConfigManager
        """
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        """
        Inicializa o gerenciador de configuração, carregando as configurações do arquivo.
        A inicialização ocorre apenas uma vez devido ao padrão Singleton.
        """
        if not self._initialized:
            self._logger = get_logger('FIIConfig')
            
            # Determina o diretório base do projeto
            self._base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            
            # Determina o caminho do arquivo de configuração
            self._config_file = os.path.join(self._base_dir, "config", "config.json")
            
            # Carrega a configuração
            self._config = self._load_config()
            
            # Marca a instância como inicializada
            self._initialized = True
    
    def _load_config(self) -> Dict[str, Any]:
        """
        Carrega a configuração do arquivo JSON ou cria o padrão se não existir.
        
        Returns:
            Dicionário com a configuração
        """
        # Criar uma cópia da configuração padrão
        config = self.DEFAULT_CONFIG.copy()
        
        # Atualizar caminhos para absolutos
        config["data_dir"] = os.path.join(self._base_dir, config["data_dir"])
        config["cert_dir"] = os.path.join(self._base_dir, config["cert_dir"])
        config["log_dir"] = os.path.join(self._base_dir, config["log_dir"])
        
        if os.path.exists(self._config_file):
            try:
                with open(self._config_file, 'r') as f:
                    # Carrega as configurações do arquivo
                    loaded_config = json.load(f)
                    
                    # Atualiza a configuração padrão com os valores carregados
                    config.update(loaded_config)
                    
                    # Garante que os caminhos sejam absolutos mesmo que tenham sido alterados no arquivo
                    if not os.path.isabs(config["data_dir"]):
                        config["data_dir"] = os.path.join(self._base_dir, config["data_dir"])
                    if not os.path.isabs(config["cert_dir"]):
                        config["cert_dir"] = os.path.join(self._base_dir, config["cert_dir"])
                    if not os.path.isabs(config["log_dir"]):
                        config["log_dir"] = os.path.join(self._base_dir, config["log_dir"])
                        
                    self._logger.info(f"Configuração carregada do arquivo: {self._config_file}")
            except Exception as e:
                self._logger.error(f"Erro ao carregar configuração: {e}")
                self._logger.warning("Usando configuração padrão")
                
                # Garante que o diretório de configuração exista
                os.makedirs(os.path.dirname(self._config_file), exist_ok=True)
                
                # Salva a configuração padrão
                with open(self._config_file, 'w') as f:
                    json.dump(config, f, indent=4)
        else:
            self._logger.info(f"Arquivo de configuração não encontrado. Criando padrão: {self._config_file}")
            
            # Garante que o diretório de configuração exista
            os.makedirs(os.path.dirname(self._config_file), exist_ok=True)
            
            # Salva a configuração padrão
            with open(self._config_file, 'w') as f:
                json.dump(config, f, indent=4)
        
        return config
    
    def reload(self) -> None:
        """
        Recarrega a configuração do arquivo JSON.
        Útil quando o arquivo de configuração foi modificado externamente.
        """
        self._config = self._load_config()
        self._logger.info("Configuração recarregada")
    
    def get_config(self) -> Dict[str, Any]:
        """
        Retorna a configuração completa.
        
        Returns:
            Dicionário com a configuração
        """
        return self._config.copy()  # Retorna uma cópia para evitar modificações acidentais
    
    def get(self, key: str, default: Any = None) -> Any:
        """
        Retorna o valor de uma chave específica na configuração.
        
        Args:
            key: Chave a ser buscada
            default: Valor padrão a ser retornado se a chave não existir
            
        Returns:
            Valor da configuração ou o valor padrão se a chave não existir
        """
        return self._config.get(key, default)
    
    def update(self, key: str, value: Any) -> None:
        """
        Atualiza um valor específico na configuração.
        
        Args:
            key: Chave a ser atualizada
            value: Novo valor
        """
        self._config[key] = value
        self._logger.debug(f"Configuração atualizada: {key} = {value}")
    
    def save(self) -> bool:
        """
        Salva a configuração atual no arquivo JSON.
        
        Returns:
            True se a configuração foi salva com sucesso, False caso contrário
        """
        try:
            with open(self._config_file, 'w') as f:
                json.dump(self._config, f, indent=4)
            self._logger.info(f"Configuração salva em: {self._config_file}")
            return True
        except Exception as e:
            self._logger.error(f"Erro ao salvar configuração: {e}")
            return False
    
    def ensure_directories(self) -> None:
        """
        Garante que os diretórios essenciais existam.
        """
        directories = [
            self._config["data_dir"],
            self._config["cert_dir"],
            self._config["log_dir"]
        ]
        
        for directory in directories:
            if not os.path.exists(directory):
                try:
                    os.makedirs(directory, exist_ok=True)
                    self._logger.info(f"Diretório criado: {directory}")
                except Exception as e:
                    self._logger.error(f"Erro ao criar diretório {directory}: {e}")


# Função de conveniência para obter a instância do gerenciador
def get_config_manager() -> ConfigManager:
    """
    Função de conveniência para obter a instância única do gerenciador de configuração.
    
    Returns:
        Instância do ConfigManager
    """
    return ConfigManager()