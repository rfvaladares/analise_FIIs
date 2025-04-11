"""
Gerenciador de calendário da B3 para o Sistema de Análise de FIIs.
Implementa o padrão Singleton para gerenciar o calendário de dias de pregão da B3.
"""

import datetime
import pandas as pd
import pandas_market_calendars as mcal

from fii_utils.logging_manager import get_logger
from fii_utils.config_manager import get_config_manager

class CalendarManager:
    """
    Gerenciador de calendário da B3 que implementa o padrão Singleton.
    Armazena em cache o calendário da B3 e fornece métodos para verificar dias úteis.
    """
    
    # Variável de classe para armazenar a instância única (Singleton)
    _instance = None
    
    def __new__(cls) -> 'CalendarManager':
        """
        Implementação do padrão Singleton. Garante que apenas uma instância da classe seja criada.
        
        Returns:
            Instância única do CalendarManager
        """
        if cls._instance is None:
            cls._instance = super(CalendarManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self) -> None:
        """
        Inicializa o gerenciador de calendário.
        A inicialização ocorre apenas uma vez devido ao padrão Singleton.
        """
        if not self._initialized:
            self._logger = get_logger('FIICalendar')
            
            # Inicializa variáveis de instância
            self._calendar_cache = None
            self._last_update = None
            
            # Marca a instância como inicializada
            self._initialized = True
    
    def get_calendar(self) -> pd.DatetimeIndex:
        """
        Obtém o calendário da B3 (dias de pregão) usando pandas_market_calendars.
        Usa o cache se disponível e válido.
        
        Returns:
            pandas.DatetimeIndex: Calendário de dias de pregão na B3
        """
        # Acessa o gerenciador de configuração
        config_manager = get_config_manager()
        cache_days = config_manager.get("calendar_cache_days", 30)
        
        now = datetime.datetime.now()
        
        # Verificar se o cache é válido
        if (self._calendar_cache is not None and 
            self._last_update is not None and 
            (now - self._last_update).days < cache_days):
            self._logger.debug("Usando calendário da B3 em cache")
            return self._calendar_cache
        
        try:
            self._logger.info("Obtendo novo calendário da B3")
            # Obter o calendário da B3
            b3 = mcal.get_calendar('BVMF')
            
            # Obter dias de pregão para um período amplo
            start_date = (now - datetime.timedelta(days=365*2)).strftime('%Y-%m-%d')  # 2 anos atrás
            end_date = (now + datetime.timedelta(days=365)).strftime('%Y-%m-%d')      # 1 ano à frente
            
            schedule = b3.schedule(start_date=start_date, end_date=end_date)
            trading_days = schedule.index
            
            # Atualizar o cache
            self._calendar_cache = trading_days
            self._last_update = now
            
            self._logger.info(f"Calendário da B3 atualizado. {len(trading_days)} dias de pregão encontrados")
            return trading_days
        except Exception as e:
            self._logger.error(f"Erro ao obter calendário da B3: {e}")
            # Caso falhe, retornar o cache antigo se disponível
            if self._calendar_cache is not None:
                self._logger.warning("Usando calendário em cache antigo devido a erro na atualização")
                return self._calendar_cache
            else:
                self._logger.error("Nenhum calendário em cache disponível. Não é possível determinar dias de pregão")
                raise
    
    def is_trading_day(self, date: datetime.date) -> bool:
        """
        Verifica se uma data é dia de pregão na B3.
        
        Args:
            date: Objeto datetime.date ou datetime.datetime
            
        Returns:
            bool: True se for dia de pregão, False caso contrário
        """
        # Converter para datetime se for date
        if isinstance(date, datetime.date) and not isinstance(date, datetime.datetime):
            date = datetime.datetime.combine(date, datetime.datetime.min.time())
        
        # Obter o calendário da B3
        b3_calendar = self.get_calendar()
        
        # Converter para Timestamp para comparação com o calendário
        data_ts = pd.Timestamp(date)
        
        # Verificar se a data está no calendário de pregão
        return data_ts in b3_calendar
    
    def get_previous_trading_day(self, date: datetime.date) -> datetime.date:
        """
        Obtém o dia de pregão na B3 anterior a uma data.
        
        Args:
            date: Objeto datetime.date ou datetime.datetime
            
        Returns:
            datetime.date: Data do dia de pregão anterior
        """
        # Converter para datetime se for date
        if isinstance(date, datetime.date) and not isinstance(date, datetime.datetime):
            date = datetime.datetime.combine(date, datetime.datetime.min.time())
        
        # Obter o calendário da B3
        b3_calendar = self.get_calendar()
        
        # Converter para Timestamp para comparação com o calendário
        data_ts = pd.Timestamp(date)
        
        # Encontrar o dia de pregão anterior
        previous_trading_days = b3_calendar[b3_calendar < data_ts]
        
        if len(previous_trading_days) > 0:
            previous_trading_day = previous_trading_days[-1]
            return previous_trading_day.date()
        else:
            self._logger.warning(f"Nenhum dia de pregão anterior a {date.strftime('%Y-%m-%d')} encontrado")
            # Retornar a data original - 1 dia como fallback
            return (date - datetime.timedelta(days=1)).date()
    
    def clear_cache(self) -> None:
        """
        Limpa o cache do calendário, forçando uma nova consulta na próxima vez.
        """
        self._calendar_cache = None
        self._last_update = None
        self._logger.info("Cache do calendário da B3 limpo")


# Função de conveniência para obter a instância do gerenciador
def get_calendar_manager() -> CalendarManager:
    """
    Função de conveniência para obter a instância única do gerenciador de calendário.
    
    Returns:
        Instância do CalendarManager
    """
    return CalendarManager()