"""
Gerenciador centralizado de cache para o Sistema de Análise de FIIs.
Implementa o padrão Singleton para fornecer um sistema de cache unificado
para toda a aplicação.
"""
import time
import threading
from typing import Dict, Any, Optional, Callable
import functools

from fii_utils.logging_manager import get_logger
from fii_utils.config_manager import get_config_manager

class CachePolicy:
    """
    Define a política de expiração e invalidação para entradas do cache.
    """
    
    def __init__(self, ttl: int = 300, max_size: int = 1000):
        """
        Inicializa uma política de cache.
        
        Args:
            ttl: Tempo de vida em segundos (Time To Live)
            max_size: Tamanho máximo do cache (número de entradas)
        """
        self.ttl = ttl
        self.max_size = max_size

class CacheEntry:
    """
    Representa uma entrada individual no cache.
    """
    
    def __init__(self, key: str, value: Any, policy: CachePolicy):
        """
        Inicializa uma entrada de cache.
        
        Args:
            key: Chave única para a entrada
            value: Valor armazenado
            policy: Política de cache aplicada a esta entrada
        """
        self.key = key
        self.value = value
        self.policy = policy
        self.created_at = time.time()
        self.last_accessed = time.time()
        self.access_count = 0
    
    def is_expired(self) -> bool:
        """
        Verifica se a entrada expirou com base em sua política de TTL.
        
        Returns:
            True se a entrada expirou, False caso contrário
        """
        return time.time() - self.created_at > self.policy.ttl
    
    def access(self) -> None:
        """
        Registra um acesso à entrada, atualizando estatísticas.
        """
        self.last_accessed = time.time()
        self.access_count += 1

class CacheManager:
    """
    Gerenciador centralizado de cache que implementa o padrão Singleton.
    Fornece um sistema de cache na memória para resultados de consultas
    frequentes e operações custosas.
    """
    
    # Instância única (Singleton)
    _instance = None
    
    def __new__(cls) -> 'CacheManager':
        """
        Implementa o padrão Singleton.
        
        Returns:
            Instância única do CacheManager
        """
        if cls._instance is None:
            cls._instance = super(CacheManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        """
        Inicializa o gerenciador de cache.
        Executado apenas uma vez devido ao padrão Singleton.
        """
        if not self._initialized:
            self.logger = get_logger('FIICache')
            self.config = get_config_manager()
            
            # Carrega configurações de cache
            self.default_ttl = self.config.get("cache_default_ttl", 300)  # 5 minutos
            self.max_size = self.config.get("cache_max_size", 1000)
            self.enable_stats = self.config.get("cache_enable_stats", True)
            
            # Inicializa caches com lock para thread-safety
            self._cache = {}  # Dict[str, CacheEntry]
            self._cache_lock = threading.RLock()
            
            # Estatísticas de cache
            self._hit_count = 0
            self._miss_count = 0
            self._eviction_count = 0
            
            # Políticas de cache por namespace
            self._policies = {}  # Dict[str, CachePolicy]
            
            # Registra política padrão
            self.register_policy('default', CachePolicy(
                ttl=self.default_ttl,
                max_size=self.max_size
            ))
            
            self._initialized = True
            self.logger.info("CacheManager inicializado")
    
    def register_policy(self, namespace: str, policy: CachePolicy) -> None:
        """
        Registra uma política de cache para um namespace específico.
        
        Args:
            namespace: Nome do espaço de cache
            policy: Política a ser aplicada
        """
        with self._cache_lock:
            self._policies[namespace] = policy
            self.logger.debug(f"Política registrada para namespace '{namespace}': TTL={policy.ttl}s, Max Size={policy.max_size}")
    
    def get_policy(self, namespace: str) -> CachePolicy:
        """
        Obtém a política de cache para um namespace.
        Se não houver política específica, retorna a política padrão.
        
        Args:
            namespace: Nome do espaço de cache
            
        Returns:
            Política de cache aplicável
        """
        with self._cache_lock:
            return self._policies.get(namespace, self._policies['default'])
    
    def _make_key(self, namespace: str, key: Any) -> str:
        """
        Cria uma chave de cache completa combinando namespace e chave.
        
        Args:
            namespace: Espaço de nomes para agrupar entradas de cache
            key: Chave original (pode ser qualquer objeto hashable)
            
        Returns:
            Chave de cache padronizada
        """
        return f"{namespace}:{hash(key)}"
    
    def get(self, namespace: str, key: Any, default: Any = None) -> Any:
        """
        Recupera um valor do cache.
        
        Args:
            namespace: Espaço de nomes do cache
            key: Chave para busca
            default: Valor padrão se a chave não existir
            
        Returns:
            Valor armazenado ou default se não encontrado
        """
        cache_key = self._make_key(namespace, key)
        
        with self._cache_lock:
            entry = self._cache.get(cache_key)
            
            if entry is None:
                # Cache miss
                if self.enable_stats:
                    self._miss_count += 1
                return default
            
            if entry.is_expired():
                # Entrada expirada
                del self._cache[cache_key]
                if self.enable_stats:
                    self._miss_count += 1
                    self._eviction_count += 1
                return default
            
            # Cache hit
            entry.access()
            if self.enable_stats:
                self._hit_count += 1
                
            return entry.value
    
    def set(self, namespace: str, key: Any, value: Any) -> None:
        """
        Armazena um valor no cache.
        
        Args:
            namespace: Espaço de nomes do cache
            key: Chave para armazenamento
            value: Valor a ser armazenado
        """
        cache_key = self._make_key(namespace, key)
        policy = self.get_policy(namespace)
        
        with self._cache_lock:
            # Verifica se o cache atingiu o tamanho máximo
            if len(self._cache) >= policy.max_size:
                self._evict_entries(namespace)
            
            # Cria nova entrada
            entry = CacheEntry(cache_key, value, policy)
            self._cache[cache_key] = entry
    
    def _evict_entries(self, namespace: str) -> None:
        """
        Remove entradas do cache quando o limite é atingido.
        Prioriza a remoção de entradas expiradas e menos acessadas.
        
        Args:
            namespace: Namespace para o qual fazer a limpeza
        """
        namespace_prefix = f"{namespace}:"
        
        # Primeiro, remove entradas expiradas
        expired_keys = []
        for key, entry in self._cache.items():
            if key.startswith(namespace_prefix) and entry.is_expired():
                expired_keys.append(key)
        
        # Remove todas as expiradas
        for key in expired_keys:
            del self._cache[key]
            self._eviction_count += 1
        
        # Se ainda for necessário, remove as menos acessadas recentemente
        if len(self._cache) >= self.get_policy(namespace).max_size:
            namespace_entries = [(k, v) for k, v in self._cache.items() 
                                if k.startswith(namespace_prefix)]
            
            # Ordena por tempo do último acesso (mais antigo primeiro)
            namespace_entries.sort(key=lambda x: x[1].last_accessed)
            
            # Remove 25% das entradas mais antigas ou pelo menos 1
            to_remove = max(1, len(namespace_entries) // 4)
            for i in range(to_remove):
                if i < len(namespace_entries):
                    del self._cache[namespace_entries[i][0]]
                    self._eviction_count += 1
    
    def invalidate(self, namespace: str, key: Optional[Any] = None) -> None:
        """
        Invalida entradas do cache.
        
        Args:
            namespace: Espaço de nomes para invalidar
            key: Chave específica ou None para invalidar todo o namespace
        """
        with self._cache_lock:
            if key is not None:
                # Invalidar chave específica
                cache_key = self._make_key(namespace, key)
                if cache_key in self._cache:
                    del self._cache[cache_key]
                    self.logger.debug(f"Invalidada entrada de cache: {cache_key}")
            else:
                # Invalidar todo o namespace
                namespace_prefix = f"{namespace}:"
                keys_to_remove = [k for k in self._cache.keys() if k.startswith(namespace_prefix)]
                
                for k in keys_to_remove:
                    del self._cache[k]
                
                self.logger.debug(f"Invalidadas {len(keys_to_remove)} entradas do namespace: {namespace}")
    
    def clear(self) -> None:
        """
        Limpa todo o cache.
        """
        with self._cache_lock:
            self._cache.clear()
            self.logger.info("Cache completamente limpo")
    
    def get_stats(self) -> Dict[str, Any]:
        """
        Retorna estatísticas de uso do cache.
        
        Returns:
            Dicionário com estatísticas
        """
        with self._cache_lock:
            total_requests = self._hit_count + self._miss_count
            hit_ratio = (self._hit_count / total_requests) * 100 if total_requests > 0 else 0
            
            # Conta entradas por namespace
            namespace_counts = {}
            for key in self._cache.keys():
                namespace = key.split(':', 1)[0]
                namespace_counts[namespace] = namespace_counts.get(namespace, 0) + 1
            
            return {
                "entries": len(self._cache),
                "hits": self._hit_count,
                "misses": self._miss_count,
                "hit_ratio": hit_ratio,
                "evictions": self._eviction_count,
                "namespaces": namespace_counts
            }

# Decorator para facilitar o uso de cache
def cached(namespace: str, key_func: Optional[Callable] = None, ttl: Optional[int] = None):
    """
    Decorator para cache de resultados de funções.
    
    Args:
        namespace: Namespace para armazenar o resultado
        key_func: Função opcional para gerar a chave do cache a partir dos argumentos
                 Se None, usa os argumentos posicionais para gerar a chave
        ttl: Tempo de vida específico para esta entrada (se None, usa o TTL do namespace)
        
    Returns:
        Decorator configurado
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache_manager = CacheManager()
            
            # Gera a chave do cache
            if key_func is not None:
                cache_key = key_func(*args, **kwargs)
            else:
                # Usa uma tupla dos argumentos como chave
                cache_key = (args, tuple(sorted(kwargs.items())))
            
            # Tenta obter do cache
            result = cache_manager.get(namespace, cache_key)
            
            if result is None:
                # Não encontrado no cache, executa a função
                result = func(*args, **kwargs)
                
                # Armazena no cache apenas se o resultado não for None
                if result is not None:
                    cache_manager.set(namespace, cache_key, result)
                    
                    # Configura TTL personalizado se especificado
                    if ttl is not None:
                        # Cria uma política específica para esta entrada
                        policy = CachePolicy(ttl=ttl)
                        entry_key = cache_manager._make_key(namespace, cache_key)
                        if entry_key in cache_manager._cache:
                            cache_manager._cache[entry_key].policy = policy
            
            return result
        return wrapper
    return decorator

# Função de conveniência para obter a instância do gerenciador
def get_cache_manager() -> CacheManager:
    """
    Retorna a instância única do gerenciador de cache.
    
    Returns:
        Instância do CacheManager
    """
    return CacheManager()