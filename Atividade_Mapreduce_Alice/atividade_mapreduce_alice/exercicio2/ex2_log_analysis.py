from typing import List, Tuple, Dict
from collections import defaultdict
from datetime import datetime
import re, os

# ==================== CONFIGURAÇÃO DO LOG ====================
LOG_PATTERN = r'(\S+) - - \[(.*?)\] "(\S+) (\S+) (\S+)" (\d+) (\d+)'

def parse_log_line(log_line: str) -> Dict:
    """Parse uma linha de log Apache."""
    match = re.match(LOG_PATTERN, log_line)
    if match:
        return {
            'ip': match.group(1),
            'timestamp': match.group(2),
            'method': match.group(3),
            'url': match.group(4),
            'protocol': match.group(5),
            'status': match.group(6),
            'size': match.group(7)
        }
    return {}

# ==================== FUNÇÕES MAP ====================
def map_urls(line: str) -> Tuple[str, int]:
    parsed = parse_log_line(line)
    return (parsed.get('url'), 1)

def map_status(line: str) -> Tuple[str, int]:
    parsed = parse_log_line(line)
    return (parsed.get('status'), 1)

def map_ips(line: str) -> Tuple[str, int]:
    parsed = parse_log_line(line)
    return (parsed.get('ip'), 1)

def map_hour(line: str) -> Tuple[int, int]:
    parsed = parse_log_line(line)
    timestamp = parsed.get('timestamp')
    DATE_FORMAT = "%d/%b/%Y:%H:%M:%S %z"
    try:
        dt = datetime.strptime(timestamp, DATE_FORMAT)
        return (dt.hour, 1)
    except:
        return (-1, 1)

# ==================== MAPREDUCE GENÉRICO ====================
def mapreduce(data: List[str], map_func, reduce_func=None) -> List[Tuple]:
    """Framework MapReduce simples."""
    mapped = [map_func(line) for line in data]
    shuffled = defaultdict(list)
    for key, val in mapped:
        if key is None or key == -1:
            continue
        shuffled[key].append(val)
    if reduce_func:
        results = [reduce_func(k, v) for k, v in shuffled.items()]
    else:
        results = [(k, sum(v)) for k, v in shuffled.items()]
    # Ordena decrescente
    if all(isinstance(v, int) for _, v in results):
        return sorted(results, key=lambda x: x[1], reverse=True)
    return results

def reduce_sum(key, values):
    return (key, sum(values))

# ==================== SCRIPT PRINCIPAL ====================
if __name__ == "__main__":
    print("="*80)
    print("EXERCÍCIO 2: ANÁLISE DE LOGS DE SERVIDOR WEB")
    print("="*80)

    # Dataset de logs de exemplo
    logs = [
        '192.168.1.100 - - [01/Jan/2025:08:30:45 +0000] "GET /index.html HTTP/1.1" 200 1234',
        '192.168.1.101 - - [01/Jan/2025:08:31:12 +0000] "GET /about.html HTTP/1.1" 200 2345',
        '192.168.1.100 - - [01/Jan/2025:08:32:33 +0000] "GET /index.html HTTP/1.1" 200 1234',
        '192.168.1.102 - - [01/Jan/2025:09:15:21 +0000] "POST /api/data HTTP/1.1" 201 567',
        '192.168.1.100 - - [01/Jan/2025:09:20:45 +0000] "GET /contact.html HTTP/1.1" 404 0',
        '192.168.1.103 - - [01/Jan/2025:10:05:33 +0000] "GET /index.html HTTP/1.1" 200 1234',
        '192.168.1.101 - - [01/Jan/2025:10:30:12 +0000] "GET /api/users HTTP/1.1" 500 0',
        '192.168.1.104 - - [01/Jan/2025:14:45:21 +0000] "GET /index.html HTTP/1.1" 200 1234',
        '192.168.1.100 - - [01/Jan/2025:14:50:33 +0000] "GET /products.html HTTP/1.1" 200 3456',
        '192.168.1.105 - - [01/Jan/2025:15:20:45 +0000] "GET /index.html HTTP/1.1" 200 1234',
    ]

    # ================= ANÁLISES =================
    # 1. URLs mais acessadas
    print("\nTOP 5 URLs MAIS ACESSADAS")
    urls = mapreduce(logs, map_urls)
    for i, (url, count) in enumerate(urls[:5], 1):
        print(f"{i}. {url:30s} : {count:3d} acessos")

    # 2. Distribuição de status HTTP
    print("\nDISTRIBUIÇÃO DE CÓDIGOS HTTP")
    status = mapreduce(logs, map_status)
    for s, c in status:
        desc = {'200':'OK','201':'Created','404':'Not Found','500':'Internal Server Error'}.get(s,'Unknown')
        print(f"HTTP {s} ({desc:20s}) : {c:3d} requisições")

    # 3. IPs com mais requisições
    print("\nTOP 5 IPs COM MAIS REQUISIÇÕES")
    ips = mapreduce(logs, map_ips)
    for i, (ip, count) in enumerate(ips[:5], 1):
        print(f"{i}. {ip:20s} : {count:3d} requisições")

    # 4. Distribuição por hora
    print("\nDISTRIBUIÇÃO DE ACESSOS POR HORA")
    hours = mapreduce(logs, map_hour)
    for hour, count in sorted(hours, key=lambda x: x[0]):
        bar = "█"*count
        print(f"{hour:02d}:00 | {bar} ({count})")

    # ================= SALVAR RESULTADOS =================
    os.makedirs("results", exist_ok=True)
    result_file = os.path.join("results", "ex2_log_results.txt")
    with open(result_file, "w") as f:
        f.write("TOP 5 URLs MAIS ACESSADAS\n")
        for url, count in urls[:5]:
            f.write(f"{url}: {count}\n")
        f.write("\nDISTRIBUIÇÃO DE CÓDIGOS HTTP\n")
        for s, c in status:
            f.write(f"{s}: {c}\n")
        f.write("\nTOP 5 IPs COM MAIS REQUISIÇÕES\n")
        for ip, count in ips[:5]:
            f.write(f"{ip}: {count}\n")
        f.write("\nDISTRIBUIÇÃO DE ACESSOS POR HORA\n")
        for hour, count in sorted(hours, key=lambda x: x[0]):
            f.write(f"{hour:02d}:00: {count}\n")

    print(f"\nResultados salvos em '{result_file}'")
