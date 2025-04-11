# Sistema de Análise de Fundos Imobiliários (FIIs)

Um sistema completo para download, processamento e análise de dados históricos de cotações de Fundos de Investimento Imobiliário (FIIs) negociados na B3.

## Índice

- [Visão Geral](#visão-geral)
- [Arquitetura do Sistema](#arquitetura-do-sistema)
- [Recursos](#recursos)
- [Requisitos](#requisitos)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Configuração Inicial](#configuração-inicial)
- [Uso](#uso)
  - [Interface Unificada (main.py)](#interface-unificada-mainpy)
  - [Download de Arquivos](#download-de-arquivos)
  - [Criação e Atualização do Banco](#criação-e-atualização-do-banco)
  - [Gerenciamento de Eventos](#gerenciamento-de-eventos)
  - [Exportação de Dados](#exportação-de-dados)
  - [Verificação e Extração de ZIPs](#verificação-e-extração-de-zips)
  - [Gerenciamento de Cache](#gerenciamento-de-cache)
  - [Informações e Estatísticas](#informações-e-estatísticas)
- [Gestão de Espaço em Disco](#gestão-de-espaço-em-disco)
- [Sistema de Cache](#sistema-de-cache)
- [Segurança](#segurança)
- [Configuração Avançada](#configuração-avançada)
- [Fluxo de Trabalho Típico](#fluxo-de-trabalho-típico)
- [Solução de Problemas](#solução-de-problemas)
- [Estrutura do Banco de Dados](#estrutura-do-banco-de-dados)

## Visão Geral

O Sistema de Análise de FIIs é uma solução completa para profissionais e entusiastas do mercado de Fundos Imobiliários, permitindo:

1. **Download automatizado** de arquivos históricos de cotações da B3
2. **Armazenamento otimizado** em banco de dados SQLite
3. **Gestão de eventos corporativos** como grupamentos e desdobramentos
4. **Análise e processamento** de dados históricos
5. **Exportação** para Excel com diferentes níveis de detalhe
6. **Economia de espaço em disco** mantendo apenas os arquivos ZIP originais
7. **Sistema de cache integrado** para melhorar o desempenho de consultas frequentes

O sistema integra componentes robustos para todas as etapas do fluxo, desde a obtenção segura dos dados até sua análise, com foco em segurança, confiabilidade e facilidade de uso.

## Arquitetura do Sistema

O sistema foi refatorado seguindo os seguintes princípios e padrões de design:

### Padrão Singleton

Os gerenciadores centrais do sistema (ConfigManager, LoggingManager, CalendarManager, CacheManager) implementam o padrão Singleton, garantindo uma única instância destes componentes em toda a aplicação. Isso proporciona:

- Acesso consistente à configuração em todos os módulos
- Centralização do sistema de logging
- Gerenciamento eficiente do calendário da B3
- Cache centralizado para otimização de desempenho

### Separação de Responsabilidades (SRP)

O código foi reorganizado para seguir o princípio de responsabilidade única:

- **Módulos Utilitários**: Funções especializadas em operações específicas
- **Gerenciadores de Banco**: Classes dedicadas para operações em tabelas específicas
- **Scripts Executáveis**: Interfaces para comandos específicos
- **Interface Unificada**: Integração de todas as funcionalidades em um único ponto de entrada

### Sistema de Logger Centralizado

Um novo sistema de logging centralizado permite:

- Configuração consistente de todos os loggers
- Rotação automática de arquivos de log
- Níveis de log configuráveis
- Logs específicos para diferentes componentes (download, banco de dados, segurança)

### Sistema de Cache Centralizado

Um sistema de cache centralizado foi implementado para:

- Reduzir acessos ao banco de dados para consultas frequentes
- Melhorar drasticamente o tempo de resposta para operações repetitivas
- Políticas de cache configuráveis por namespace
- Monitoramento detalhado de uso e eficiência

### Context Managers

Implementação de context managers para operações de banco de dados, garantindo:

- Fechamento adequado de conexões
- Tratamento de exceções
- Maior legibilidade do código

### Função Utilitárias Centralizadas

Centralização de funções comuns em módulos utilitários, reduzindo duplicação de código e aumentando a manutenibilidade.

## Recursos

### Módulo de Download
- Download seguro de arquivos históricos da B3 com verificação SSL
- Detecção automática de dias úteis usando calendário oficial da B3
- Suporte para arquivos diários, mensais e anuais
- Detecção inteligente dos arquivos necessários
- Certificate pinning e monitoramento de segurança
- Recuperação automática em caso de falhas

### Processamento de Dados
- Parsing eficiente de arquivos da B3
- Processamento paralelo para arquivos grandes
- Detecção e extração automática de arquivos ZIP
- Rastreamento de arquivos processados com hash MD5
- Remoção automática de arquivos TXT após processamento
- Controle de integridade baseado em hashes

### Gestão de Banco de Dados
- Esquema otimizado para consultas de cotações
- Suporte para eventos corporativos (grupamentos e desdobramentos)
- Controle de arquivos processados com hashes
- Otimizações específicas para SQLite (WAL, cache, timeouts)
- Proteção contra concorrência e bloqueios
- Sistema de cache para consultas frequentes

### Análise e Exportação
- Exportação para Excel em diversos formatos
- Ajustes automáticos para eventos corporativos
- Consolidação de tickers históricos
- Estatísticas detalhadas dos dados
- Validação e limpeza de dados

### Gestão de Armazenamento
- Manutenção apenas dos arquivos ZIP originais para economia de espaço
- Extração temporária para processamento
- Verificação e recuperação de arquivos pendentes
- Sistema de hash baseado nos ZIPs para verificação de integridade

### Sistema de Cache
- Armazenamento em memória de resultados de consultas frequentes
- Políticas de TTL (Time To Live) configuráveis por namespace
- Invalidação seletiva quando dados são modificados
- Estatísticas detalhadas de uso e eficiência
- Interface de linha de comando para gerenciamento

## Requisitos

### Requisitos de Sistema
- Python 3.6 ou superior
- SQLite 3
- curl - para download seguro de arquivos
- OpenSSL - para verificação de certificados SSL

### Bibliotecas Python Necessárias
- pandas e numpy - para processamento e análise de dados
- pandas_market_calendars - para obter o calendário oficial da B3
- openpyxl - para exportação em formato Excel
- Bibliotecas padrão do Python (json, zipfile, logging, etc.)

### Instalação de Dependências

**Ubuntu/Debian**:
```bash
sudo apt update
sudo apt install curl openssl python3-pip python3-venv
pip3 install pandas numpy openpyxl pandas_market_calendars
```

**Fedora/CentOS**:
```bash
sudo dnf install curl openssl python3-pip
pip3 install pandas numpy openpyxl pandas_market_calendars
```

**macOS** (usando Homebrew):
```bash
brew install curl openssl python3
pip3 install pandas numpy openpyxl pandas_market_calendars
```

**Windows**:
- Instale o [Python](https://www.python.org/downloads/)
- Instale o [Git for Windows](https://gitforwindows.org/) que inclui o curl
- Ou use o WSL (Windows Subsystem for Linux) e siga as instruções para Linux
- Execute os comandos:
```
pip install pandas numpy openpyxl pandas_market_calendars
```

## Estrutura do Projeto

A estrutura do projeto foi reorganizada para maior clareza e manutenibilidade:

```
analise_FIIs/
├── fii_utils/                     # Utilitários e funções centralizadas
│   ├── __init__.py
│   ├── arquivo_utils.py           # Funções para manipulação de arquivos
│   ├── calendar_manager.py        # Gerenciador de calendário da B3 (Singleton)
│   ├── cache_manager.py           # Gerenciador de cache em memória (Singleton)
│   ├── cli_utils.py               # Utilitários para interface de linha de comando
│   ├── config_manager.py          # Gerenciador centralizado de configuração (Singleton)
│   ├── db_decorators.py           # Decoradores para otimização de operações de BD
│   ├── db_operations.py           # Operações comuns de banco de dados
│   ├── db_utils.py                # Utilitários para banco de dados
│   ├── download_utils.py          # Utilitários para download
│   ├── downloader.py              # Módulo de download da B3
│   ├── logging_manager.py         # Gerenciador centralizado de logging (Singleton)
│   ├── parsers.py                 # Classes para parsing de arquivos
│   └── zip_utils.py               # Utilitários para manipulação de ZIPs
│
├── db_managers/                   # Gerenciadores específicos de tabelas
│   ├── __init__.py
│   ├── arquivos.py                # Gerenciador da tabela arquivos_processados
│   ├── cotacoes.py                # Gerenciador da tabela cotacoes
│   ├── eventos.py                 # Gerenciador da tabela eventos_corporativos
│   └── exportacao.py              # Gerenciador de exportação de cotações
│
├── scripts/                       # Scripts executáveis individuais
│   ├── __init__.py
│   ├── create_database.py         # Script para criar o banco
│   ├── update_database.py         # Script para atualizar o banco
│   ├── manage_eventos.py          # Script para gerenciar eventos
│   └── export_cotacoes.py         # Script para exportar cotações
│
├── config/                        # Arquivos de configuração
│   ├── config.json                # Configuração centralizada
│   └── certificates/              # Certificados para conexão segura
│
├── logs/                          # Logs do sistema (organizados por componente)
│
├── main.py                        # Interface unificada integrada
├── setup.py                       # Script de configuração inicial
└── historico_cotacoes/            # Pasta com arquivos de cotação (.ZIP)
```

## Configuração Inicial

1. Execute o script de configuração inicial para criar a estrutura de diretórios e instalar dependências:

```bash
python setup.py
```

2. Crie ou atualize o arquivo `eventos.json` na raiz do projeto com os eventos corporativos no formato:

```json
[
    {"codigo": "GARE11", "evento": "desdobramento", "data": "2022-08-30", "fator": 10},
    {"codigo": "HGCR11", "evento": "desdobramento", "data": "2017-10-10", "fator": 10},
    {"codigo": "KNSC11", "evento": "desdobramento", "data": "2023-11-06", "fator": 10},
    {"codigo": "VGIR11", "evento": "desdobramento", "data": "2022-09-09", "fator": 10},
    {"codigo": "VRTA11", "evento": "grupamento", "data": "2011-06-06", "fator": 100},
    {"codigo": "RZAT11", "evento": "desdobramento", "data": "2021-01-27", "fator": 10},
    {"codigo": "HGBS11", "evento": "desdobramento", "data": "2018-04-27", "fator": 10}
]
```

3. Para exportar cotações, crie um arquivo `fundos.json` com a lista de FIIs de interesse:

```json
{
    "fundos": [
        "RZTR11", 
        ["GALG11", "GARE11"],
        "TRXF11", 
        "HGCR11", 
        "KNIP11",
        "KNSC11"
    ]
}
```

Nota: Quando um FII já tiver passado por uma mudança de ticker, liste todos os seus tickers como um array, com o ticker atual (mais recente) na última posição.

## Uso

### Interface Unificada (main.py)

O sistema agora oferece uma interface unificada através do script `main.py`. Este script integra todas as funcionalidades em um único ponto de entrada, com subcomandos específicos para cada operação:

```bash
# Sintaxe geral
python main.py <operação> [opções]

# Mostra a ajuda geral
python main.py --help

# Mostra ajuda para uma operação específica
python main.py <operação> --help
```

Operações disponíveis:
- `criar` - Cria o banco de dados e processa arquivos históricos
- `atualizar` - Atualiza o banco com arquivos novos ou modificados
- `eventos` - Gerencia eventos corporativos
- `exportar` - Exporta cotações para Excel
- `download` - Baixa arquivos da B3
- `extrair` - Verifica e extrai arquivos ZIP pendentes
- `cache` - Gerencia o sistema de cache em memória
- `info` - Exibe informações sobre o banco de dados

### Download de Arquivos

```bash
# Baixar automaticamente o(s) arquivo(s) necessário(s) com base no estado do banco
python main.py download --auto

# Baixar dados para uma data específica
python main.py download --data 18/03/2025

# Baixar dados para múltiplas datas específicas
python main.py download --data 18/03/2025 19/03/2025 20/03/2025

# Baixar dados para um intervalo de datas
python main.py download --range 15/03/2025 18/03/2025

# Baixar dados do dia útil anterior à data atual
python main.py download --anterior

# Baixar arquivos e atualizar o banco de dados em seguida
python main.py download --auto --atualizar

# Verificar e extrair ZIPs pendentes durante o download
python main.py download --auto --verificar-zips

# Verificar segurança do ambiente antes do download
python main.py download --verificar --data 18/03/2025

# Limpar certificados antigos e baixar dados
python main.py download --limpar-certs --auto
```

### Criação e Atualização do Banco

```bash
# Criar banco de dados, tabelas e processar arquivos históricos
python main.py criar

# Atualizar com arquivos já na pasta historico_cotacoes
python main.py atualizar

# Baixar novos arquivos e atualizar o banco (tudo em um comando)
python main.py atualizar --download

# Verificar e extrair ZIPs pendentes durante a atualização
python main.py atualizar --verificar-zips
```

### Gerenciamento de Eventos

```bash
# Criar tabela de eventos corporativos
python main.py eventos criar

# Importar eventos de arquivo JSON
python main.py eventos importar --arquivo eventos.json

# Listar todos os eventos
python main.py eventos listar

# Listar eventos de um FII específico
python main.py eventos listar --codigo HGCR11
```

Também é possível usar o script específico para operações mais avançadas:

```bash
# Adicionar um novo evento individual
python scripts/manage_eventos.py adicionar --codigo XPLG11 --evento desdobramento --data 2023-05-22 --fator 10

# Atualizar fator de um evento existente
python scripts/manage_eventos.py atualizar --codigo XPLG11 --evento desdobramento --data 2023-05-22 --fator 8

# Remover um evento
python scripts/manage_eventos.py remover --codigo XPLG11 --evento desdobramento --data 2023-05-22

# Listar eventos em um período específico
python scripts/manage_eventos.py listar --periodo 2022-01-01 2022-12-31
```

### Exportação de Dados

```bash
# Exportar apenas preços de fechamento (sem ajuste)
python main.py exportar --json fundos.json --saida cotacoes.xlsx

# Exportar preços de fechamento com ajuste para eventos corporativos
python main.py exportar --json fundos.json --saida cotacoes.xlsx --ajustar

# Exportar todos os dados (abertura, máxima, mínima, fechamento, volume)
python main.py exportar --json fundos.json --saida cotacoes.xlsx --completo

# Exportar todos os dados com ajuste para eventos corporativos
python main.py exportar --json fundos.json --saida cotacoes.xlsx --completo --ajustar
```

Obs: Os arquivos de saída terão sufixos adicionados ao nome para indicar o tipo de exportação:
- `_fechamento` para apenas preços de fechamento
- `_completo` para todos os dados
- `_ajustado` quando os preços forem ajustados por eventos corporativos

### Verificação e Extração de ZIPs

O sistema agora oferece comandos específicos para verificar e extrair arquivos ZIP pendentes:

```bash
# Verificar e extrair arquivos ZIP pendentes
python main.py extrair

# Verificar ZIPs durante atualização
python main.py atualizar --verificar-zips

# Verificar ZIPs durante download
python main.py download --verificar-zips
```

Estes comandos são úteis quando:
- Há muitos arquivos ZIP na pasta que ainda não foram processados
- Você transferiu arquivos ZIP de outro sistema
- Houve interrupção do processamento anterior
- Você suspeita que alguns dados podem estar faltando no banco

### Gerenciamento de Cache

O sistema implementa um cache em memória para melhorar desempenho. Você pode gerenciá-lo com os seguintes comandos:

```bash
# Ver estatísticas detalhadas do cache
python main.py cache --stats

# Limpar completamente o cache
python main.py cache --clear

# Invalidar um namespace específico do cache
python main.py cache --invalidate cotacoes_lista
```

O sistema mantém estatísticas detalhadas sobre o uso do cache, incluindo:
- Total de entradas em cache
- Taxa de acertos (hit ratio)
- Número de evicções
- Distribuição de entradas por namespace

### Informações e Estatísticas

```bash
# Ver estatísticas gerais do banco
python main.py info
```

Este comando mostra informações detalhadas sobre o banco de dados, incluindo:
- Total de registros de cotações
- Total de FIIs no banco
- Período de dados disponíveis
- Quantidade de arquivos processados por tipo
- Eventos corporativos registrados
- Estatísticas do sistema de cache

## Gestão de Espaço em Disco

O sistema foi otimizado para economizar espaço em disco mantendo apenas os arquivos ZIP originais da B3, eliminando a necessidade de armazenar os arquivos TXT extraídos.

### Como Funciona

1. **Download**: Os arquivos ZIP são baixados da B3
2. **Extração Temporária**: Os arquivos são extraídos para TXT apenas durante o processamento
3. **Processamento**: Os dados são inseridos no banco de dados
4. **Registro otimizado**: O sistema registra o *nome do arquivo ZIP* e o *hash do ZIP* (não mais do TXT)
5. **Remoção automática**: O arquivo TXT é automaticamente removido após o processamento
6. **Verificação de integridade**: Verificações posteriores são baseadas nos hashes dos ZIPs

### Vantagens desta Abordagem

- **Economia de espaço**: Os arquivos TXT são tipicamente 3-4x maiores que os ZIPs correspondentes
- **Preservação de dados originais**: Os arquivos ZIP originais da B3 são mantidos para referência
- **Integridade garantida**: O sistema verifica modificações usando os hashes dos ZIPs
- **Automatização completa**: Nenhuma intervenção manual necessária para gerenciar arquivos

### Migração de Sistemas Existentes

Se você estava usando uma versão anterior do sistema que mantinha os arquivos TXT:

1. Você pode manter apenas os ZIPs e remover manualmente os TXTs
2. O sistema continuará funcionando normalmente, usando os ZIPs como referência
3. Caso necessário, execute `python main.py extrair` para verificar se algum ZIP não foi processado

## Sistema de Cache

O sistema implementa um mecanismo de cache em memória para otimizar o desempenho, reduzindo significativamente o tempo de resposta para operações frequentes e diminuindo a carga no banco de dados.

### Como Funciona

1. **Armazenamento em Memória**: Os resultados de consultas frequentes são armazenados em memória
2. **TTL (Time To Live)**: Cada entrada tem um tempo de vida configurável após o qual é invalidada
3. **Políticas por Namespace**: Diferentes tipos de dados podem ter políticas de cache distintas
4. **Invalidação Seletiva**: Quando dados são modificados, apenas as entradas de cache relevantes são invalidadas
5. **Monitoramento de Uso**: Sistema mantém estatísticas detalhadas de uso e eficiência

### Principais Namespaces de Cache

O sistema utiliza os seguintes namespaces principais:

- `cotacoes_lista`: Cache de listas de FIIs (TTL: 1 hora)
- `cotacoes_ultima_data`: Cache da última data de cotação (TTL: 10 minutos)
- `cotacoes_estatisticas`: Cache de estatísticas de cotações (TTL: 30 minutos)
- `arquivos_processados`: Cache de status de arquivos (TTL: 30 minutos)
- `eventos_corporativos`: Cache de eventos corporativos (TTL: 1 hora)
- `exportacao_fiis`: Cache de listas de FIIs para exportação (TTL: 30 minutos)

### Gerenciamento do Cache

O cache é gerenciado automaticamente pelo sistema, mas você pode controlá-lo manualmente:

```bash
# Ver estatísticas do uso do cache
python main.py cache --stats

# Limpar todo o cache (útil se suspeitar de dados inconsistentes)
python main.py cache --clear

# Invalidar apenas um namespace específico
python main.py cache --invalidate cotacoes_lista
```

### Benefícios do Sistema de Cache

- **Desempenho Significativamente Melhor**: Operações frequentes são até 10x mais rápidas
- **Redução da Carga no Banco de Dados**: Menos consultas SQL para dados que mudam raramente
- **Melhor Experiência de Usuário**: Redução perceptível no tempo de resposta
- **Escalabilidade**: Sistema lida melhor com grandes volumes de dados
- **Transparência**: Estatísticas detalhadas permitem monitorar e otimizar o uso do cache

## Segurança

O módulo de download implementa várias medidas de segurança:

### Certificate Pinning

O sistema verifica a impressão digital (fingerprint) do certificado SSL do servidor da B3 para detectar possíveis ataques man-in-the-middle. A impressão digital é armazenada e verificada em execuções futuras.

### Monitoramento de Mudanças em Certificados

Quando a impressão digital do certificado muda:
1. Um alerta é registrado nos logs de segurança (`logs/b3_security.log`)
2. A mudança é registrada em um histórico
3. O usuário é notificado, mas a operação continua (configurável)

### Verificação de Arquivos

Os arquivos baixados são verificados para garantir:
- Arquivo ZIP válido
- Conteúdo não vazio
- Tamanho adequado (avisos para arquivos muito pequenos)

### Permissões de Diretórios

Em sistemas Unix/Linux, o sistema pode verificar e corrigir permissões inseguras de diretórios.

```bash
# Verificar e corrigir permissões de diretórios
python main.py download --fix-permissions
```

## Configuração Avançada

O sistema agora utiliza um gerenciador centralizado de configuração (`ConfigManager`) que mantém todas as configurações em um único local: `config/config.json`.

### Opções de Configuração

```json
{
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
  "verify_downloads": true,
  "concurrent_downloads": 1,
  "enable_curl_verbose": true,
  "fix_permissions": false,
  "secure_permissions": "750",
  "extract_retries": 3,
  "extract_retry_delay": 2.0,
  "calendar_cache_days": 30,
  "cache_default_ttl": 300,
  "cache_max_size": 1000,
  "cache_enable_stats": true,
  "db_lote_size_pequeno": 1000,
  "db_lote_size_medio": 5000,
  "db_lote_size_grande": 10000,
  "db_tamanho_maximo_lote_bytes": 1048576,
  "db_timeout": 60.0
}
```

### Descrição das Configurações

| Opção | Descrição |
|-------|-----------|
| `base_url` | URL base para download dos arquivos |
| `data_dir` | Diretório para salvar os arquivos baixados |
| `cert_dir` | Diretório para armazenar certificados |
| `log_dir` | Diretório para arquivos de log |
| `max_retries` | Número máximo de tentativas em caso de falha |
| `backoff_factor` | Fator de espera entre tentativas |
| `wait_between_downloads` | Intervalo de espera entre downloads [min, max] |
| `cert_rotation_days` | Dias após os quais certificados antigos são removidos |
| `user_agent` | User-Agent usado nas requisições HTTP |
| `log_level` | Nível de detalhe dos logs (INFO, DEBUG, WARNING, ERROR) |
| `max_log_size_mb` | Tamanho máximo dos arquivos de log antes da rotação |
| `max_log_backups` | Número de backups de logs antigos a manter |
| `verify_downloads` | Se deve verificar os arquivos baixados |
| `concurrent_downloads` | Número de downloads simultâneos (aumentar com cautela) |
| `enable_curl_verbose` | Ativar saída detalhada do curl para diagnóstico |
| `fix_permissions` | Corrigir automaticamente permissões inseguras |
| `secure_permissions` | Permissões seguras para diretórios (formato octal) |
| `extract_retries` | Número de tentativas para extrair um arquivo ZIP |
| `extract_retry_delay` | Tempo de espera entre tentativas de extração |
| `calendar_cache_days` | Período de validade do cache do calendário B3 (dias) |
| `cache_default_ttl` | TTL padrão para entradas de cache em segundos (5 minutos) |
| `cache_max_size` | Número máximo de entradas no cache (por namespace) |
| `cache_enable_stats` | Ativar coleta de estatísticas de uso do cache |
| `db_lote_size_pequeno` | Tamanho de lote para pequenas quantidades de registros |
| `db_lote_size_medio` | Tamanho de lote para quantidades médias de registros |
| `db_lote_size_grande` | Tamanho de lote para grandes quantidades de registros |
| `db_tamanho_maximo_lote_bytes` | Tamanho máximo em bytes para lotes de inserção |
| `db_timeout` | Timeout em segundos para operações de banco de dados |

## Fluxo de Trabalho Típico

### Primeira Execução
1. Configure o sistema:
   ```bash
   python setup.py
   ```

2. Crie o arquivo `eventos.json` com os dados dos eventos corporativos
   
3. Baixe os dados e crie o banco:
   ```bash
   # Opção unificada (mais simples):
   python main.py download --auto
   python main.py criar
   python main.py eventos importar --arquivo eventos.json
   
   # Alternativa com scripts individuais:
   python scripts/download.py --auto
   python scripts/create_database.py
   python scripts/manage_eventos.py importar --arquivo eventos.json
   ```

### Uso Regular
1. Atualizar com novos dados da B3:
   ```bash
   # Opção 1: Download automático + atualização em um único comando
   python main.py atualizar --download
   
   # Opção 2: Controle manual das datas + atualização
   python main.py download --data 18/03/2025 --atualizar
   
   # Opção 3: Processo em duas etapas
   python main.py download --auto  # ou outras opções de data
   python main.py atualizar
   ```

2. Se tiver novos eventos corporativos:
   - Adicione-os ao `eventos.json` e execute:
     ```bash
     python main.py eventos importar --arquivo eventos.json
     ```

3. Monitoramento e gerenciamento do cache:
   ```bash
   # Verificar estatísticas do cache (útil para diagnóstico de desempenho)
   python main.py cache --stats
   
   # Limpar o cache se necessário (após muitas operações ou modificações)
   python main.py cache --clear
   ```

### Verificação e Recuperação
Se houver suspeita de arquivos não processados ou inconsistências:

```bash
# Verifica se há ZIPs pendentes e extrai-os
python main.py extrair

# Atualiza o banco com os dados dos novos TXT extraídos
python main.py atualizar

# Em caso de problemas persistentes, limpe o cache
python main.py cache --clear
```

### Exportação de Dados para Análise
1. Crie um arquivo `fundos.json` com a lista de FIIs para análise
2. Exporte os dados conforme necessário:
   ```bash
   # Para análise técnica (inclui todos os dados)
   python main.py exportar --json fundos.json --saida analise_tecnica.xlsx --completo
   
   # Para análise de longo prazo (com ajustes para eventos corporativos)
   python main.py exportar --json fundos.json --saida analise_longo_prazo.xlsx --ajustar
   ```

## Solução de Problemas

### Logs do Sistema

O sistema agora utiliza um sistema de logging centralizado com logs organizados por componente:

- `logs/fii_main.log` - Log principal do sistema
- `logs/b3_downloader.log` - Log de operações de download
- `logs/b3_security.log` - Log de eventos de segurança
- `logs/FIIDatabase.log` - Log de operações de banco de dados
- `logs/FIICache.log` - Log do sistema de cache
- `logs/system.log` - Log de eventos do sistema

Para diagnóstico, você pode aumentar o nível de detalhamento dos logs alterando o arquivo `config/config.json`:
```json
{
  "log_level": "DEBUG",
  "enable_curl_verbose": true
}
```

### Erros Comuns

#### Erro de Certificado SSL

**Problema**: Falha na verificação do certificado SSL

**Solução**:
1. Verifique se o OpenSSL está atualizado
2. O sistema tentará contornar esse problema usando o certificado local
3. Execute com a opção `--verificar` para diagnóstico

```bash
# Verificar versão do OpenSSL
openssl version

# Executar download com verificação de ambiente
python main.py download --verificar --data 18/03/2025
```

#### Arquivo ZIP Inválido

**Problema**: O arquivo baixado não é um ZIP válido

**Possíveis causas**:
1. O servidor retornou uma página de erro em HTML
2. O servidor retornou um arquivo corrompido
3. Problemas na conexão durante o download

**Solução**:
1. Verifique os logs em `logs/b3_downloader.log`
2. Tente novamente mais tarde, pode ser um problema temporário
3. Verifique se há problemas de rede ou firewall

#### ZIPs Não Extraídos

**Problema**: Existem arquivos ZIP que não foram extraídos ou processados

**Solução**:
1. Execute o comando específico para verificação de ZIPs pendentes:
   ```bash
   python main.py extrair
   ```
2. Verifique os logs para identificar arquivos com problemas
3. Para forçar o reprocessamento de um arquivo específico, remova sua entrada da tabela `arquivos_processados` (uso avançado)

#### Banco de Dados Bloqueado

**Problema**: Erro "database is locked" ao tentar acessar o banco de dados

**Solução**:
1. O sistema agora utiliza o modo WAL (Write-Ahead Logging) do SQLite, que reduz significativamente os bloqueios
2. Se o problema persistir, verifique se há outros processos usando o mesmo banco
3. Aumente o timeout de lock no arquivo `config/config.json`:
   ```json
   {
     "db_timeout": 60.0
   }
   ```

#### Problemas de Desempenho

**Problema**: Operações estão lentas ou o sistema está consumindo muito recurso

**Solução**:
1. Verifique o uso do cache com o comando `python main.py cache --stats`
2. Se a taxa de acertos (hit ratio) estiver baixa, considere aumentar os TTLs de cache:
   ```json
   {
     "cache_default_ttl": 600
   }
   ```
3. Se o problema persistir, tente limpar e reiniciar o cache:
   ```bash
   python main.py cache --clear
   ```
4. Para operações intensivas, aumente os tamanhos de lote em `config.json`:
   ```json
   {
     "db_lote_size_grande": 15000
   }
   ```

## Estrutura do Banco de Dados

O sistema cria um banco de dados SQLite com as seguintes tabelas:

### Tabela `cotacoes`
Armazena o histórico de cotações dos FIIs:
```sql
CREATE TABLE cotacoes (
    data TEXT,
    codigo TEXT,
    abertura REAL,
    maxima REAL,
    minima REAL,
    fechamento REAL,
    volume REAL,
    negocios INTEGER,
    quantidade INTEGER,
    PRIMARY KEY (data, codigo)
);
```

### Tabela `arquivos_processados`
Controla os arquivos que já foram processados:
```sql
CREATE TABLE arquivos_processados (
    nome_arquivo TEXT PRIMARY KEY,
    tipo TEXT,
    data_processamento TEXT,
    registros_adicionados INTEGER,
    hash_md5 TEXT
);
```

**Nota importante:** Esta tabela agora armazena referências aos arquivos ZIP (não mais TXT) junto com seus hashes para verificação de integridade.

### Tabela `eventos_corporativos`
Armazena os eventos de grupamento e desdobramento:
```sql
CREATE TABLE eventos_corporativos (
    codigo TEXT,
    data TEXT,
    tipo_evento TEXT CHECK(tipo_evento IN ('grupamento', 'desdobramento')),
    fator REAL CHECK(fator > 0),
    data_registro TEXT,
    PRIMARY KEY (codigo, data, tipo_evento)
);
```

### Otimizações do SQLite

Para melhorar a performance e reduzir bloqueios, o sistema utiliza as seguintes otimizações:

```sql
PRAGMA synchronous = NORMAL;     -- Equilíbrio entre segurança e performance
PRAGMA journal_mode = WAL;       -- Write-Ahead Logging para reduzir bloqueios
PRAGMA cache_size = 100000;      -- Cache maior para melhor performance
PRAGMA temp_store = MEMORY;      -- Armazenamento temporário em memória
PRAGMA busy_timeout = 30000;     -- Timeout de 30 segundos para bloqueios
PRAGMA page_size = 4096;         -- Tamanho de página otimizado
```