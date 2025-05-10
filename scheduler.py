#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Планировщик запуска sheet_transfer.py

Выполняет запуск скрипта sheet_transfer.py каждый час (3600 секунд).
Первый запуск происходит сразу после старта планировщика.
"""

import time
import subprocess
import logging
import signal
import sys
import os

# Настройка логирования для планировщика
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# Интервал запуска скрипта в секундах (1 час)
INTERVAL_SECONDS = 3600 

# Флаг для отслеживания запроса на завершение
terminate = False

def run_transfer_script():
    """
    Запускает основной скрипт sheet_transfer.py.
    Выводит логи скрипта напрямую в консоль.
    
    Returns:
        bool: True если скрипт выполнился успешно, False в случае ошибки
    """
    try:
        logger.info("Запуск скрипта sheet_transfer.py")
        
        # Полный путь к скрипту (можно заменить на абсолютный, если необходимо)
        script_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sheet_transfer.py")
        
        # Запуск скрипта как отдельного процесса
        # Параметры stdout=None, stderr=None позволяют видеть вывод в консоли
        result = subprocess.run(
            [sys.executable, script_path],
            stdout=None,
            stderr=None
        )
        
        # Проверка успешного выполнения
        if result.returncode == 0:
            logger.info("Скрипт успешно выполнен")
            return True
        else:
            logger.error(f"Ошибка при выполнении скрипта (код возврата: {result.returncode})")
            return False
            
    except Exception as e:
        logger.error(f"Ошибка при запуске скрипта: {e}")
        return False

def signal_handler(sig, frame):
    """
    Обработчик сигналов для корректного завершения работы планировщика.
    """
    global terminate
    logger.info("Получен сигнал на завершение работы. Завершаем планировщик...")
    terminate = True

def main():
    """
    Основная функция планировщика.
    """
    logger.info(f"Запуск планировщика для sheet_transfer.py с интервалом {INTERVAL_SECONDS} секунд ({INTERVAL_SECONDS/60} минут)")
    
    # Регистрируем обработчик сигналов для корректного завершения
    signal.signal(signal.SIGINT, signal_handler)
    
    # Запускаем скрипт сразу при старте планировщика
    logger.info("Выполняем первый запуск скрипта при старте планировщика")
    run_transfer_script()
    
    logger.info("Планировщик запущен. Для завершения нажмите Ctrl+C")
    
    # Время последнего запуска
    last_run_time = time.time()
    
    # Основной цикл планировщика
    global terminate
    while not terminate:
        # Текущее время
        current_time = time.time()
        
        # Проверяем, прошел ли интервал времени с последнего запуска
        if current_time - last_run_time >= INTERVAL_SECONDS:
            run_transfer_script()
            last_run_time = time.time()
            
        # Спим 5 минут для снижения нагрузки на процессор
        # Проверяем флаг завершения каждые 5 секунд
        for _ in range(60):  # 60 * 5 = 300 секунд (5 минут)
            if terminate:
                break
            time.sleep(5)
    
    logger.info("Планировщик завершил работу")

if __name__ == "__main__":
    main()
