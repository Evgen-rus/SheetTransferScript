import os
import sys
import logging
import datetime
import pytz
import urllib.parse
import re
import argparse
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account

# Константы
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
TARGET_DOMAIN = 'forum-info.ru'
FIRST_MONTH = 'Май 2025'

# Словарь месяцев на русском языке
RUSSIAN_MONTHS = {
    1: 'Январь',
    2: 'Февраль',
    3: 'Март',
    4: 'Апрель',
    5: 'Май',
    6: 'Июнь',
    7: 'Июль',
    8: 'Август',
    9: 'Сентябрь',
    10: 'Октябрь',
    11: 'Ноябрь',
    12: 'Декабрь'
}

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

def set_debug_logging():
    """
    Включает подробное логирование для отладки.
    """
    logger.setLevel(logging.DEBUG)
    # Добавляем больше информации в формат вывода
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s'
        ))
    logger.debug("Включен режим отладки")

def create_sheets_service():
    """
    Создает и возвращает сервис Google Sheets API.
    
    Returns:
        Объект сервиса Google Sheets API
    """
    try:
        credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE')
        credentials = service_account.Credentials.from_service_account_file(
            credentials_file, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=credentials)
        return service
    except Exception as e:
        logger.error(f"Ошибка при создании сервиса Sheets API: {e}")
        raise

def get_source_data(service, spreadsheet_id, sheet_name=None):
    """
    Получает все данные из таблицы-источника.
    
    Args:
        service: Сервис Google Sheets API
        spreadsheet_id: ID таблицы-источника
        sheet_name: Название вкладки (если None, используется первая вкладка)
        
    Returns:
        Список строк с данными
    """
    try:
        # Получение информации о таблице
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        
        # Если название вкладки указано, ищем её
        if sheet_name:
            sheet_found = False
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['title'] == sheet_name:
                    sheet_title = sheet_name
                    sheet_found = True
                    logger.info(f"Используем указанную вкладку источника: '{sheet_title}'")
                    break
                    
            if not sheet_found:
                logger.warning(f"Вкладка '{sheet_name}' не найдена в исходной таблице. Доступные вкладки:")
                for sheet in spreadsheet['sheets']:
                    logger.warning(f"  - {sheet['properties']['title']}")
                logger.info("Используем первую вкладку по умолчанию")
                sheet_title = spreadsheet['sheets'][0]['properties']['title']
        else:
            # Если название не указано, используем первую вкладку
            sheet_title = spreadsheet['sheets'][0]['properties']['title']
            logger.info(f"Используем первую вкладку источника: '{sheet_title}'")
        
        # Получение всех данных
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_title}"
        ).execute()
        
        return result.get('values', [])
    except Exception as e:
        logger.error(f"Ошибка при получении данных из источника: {e}")
        raise

def is_domain_in_url(url, domain):
    """
    Проверяет, содержится ли домен в URL.
    Учитывает протоколы, поддомены и пути.
    
    Args:
        url: Строка с URL
        domain: Домен для проверки
        
    Returns:
        True, если домен найден в URL, иначе False
    """
    try:
        if not url:
            return False
            
        # Логируем исходный URL для отладки
        logger.debug(f"Проверка домена {domain} в URL: {url}")
            
        # Преобразуем URL и домен в нижний регистр для сравнения
        url = url.lower()
        domain = domain.lower()
        
        # Очищаем URL от возможных префиксов и лишних символов
        # Например, "@https://forum-info.ru/..." или "#https://forum-info.ru/..."
        url = url.strip()
        
        # Удаляем любые префиксы перед http или www
        prefixes = ['@', '#', ' ', ',', ';', '"', "'"]
        for prefix in prefixes:
            if url.startswith(prefix):
                url = url[1:].strip()
                
        # Простая проверка на вхождение строки (быстрый чек)
        if domain not in url:
            logger.debug(f"Домен {domain} не найден в URL {url} при простой проверке")
            return False
            
        # Обработка URL с кавычками, пробелами и другими разделителями
        if any(sep in url for sep in ['"', "'", ' ', ',', ';']):
            parts = re.split(r'["\'\\s,;]', url)
            for part in parts:
                if domain in part:
                    url = part
                    logger.debug(f"Домен найден в части URL: {part}")
                    break
                    
        # Добавляем протокол, если его нет
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
            
        logger.debug(f"Подготовленный URL для проверки: {url}")
        
        try:
            parsed_url = urllib.parse.urlparse(url)
            hostname = parsed_url.netloc.lower()
            
            logger.debug(f"Извлеченный hostname: {hostname}")
            
            # Проверка на точное совпадение или поддомен
            if hostname == domain or hostname.endswith('.' + domain):
                logger.debug(f"Домен найден в hostname: {hostname}")
                return True
                
            # Проверка на вхождение домена в netloc
            if domain in hostname:
                logger.debug(f"Домен найден в netloc: {hostname}")
                return True
                
            # Дополнительная проверка - если hostname не содержит доменное имя, 
            # но путь или другая часть URL содержит его
            if domain in url:
                logger.debug(f"Домен найден в полном URL: {url}")
                return True
                
            logger.debug(f"Домен {domain} не найден в URL {url}")
            return False
        except Exception as e:
            logger.warning(f"Ошибка при парсинге URL '{url}': {e}")
            # Запасной вариант - простая проверка на вхождение
            return domain in url
            
    except Exception as e:
        logger.warning(f"Общая ошибка при анализе URL '{url}': {e}")
        # Запасной вариант - простая проверка на вхождение
        return domain in url

def get_url_from_row(row, column_index, target_domain=None):
    """
    Получает URL из строки данных.
    Пытается обработать разные форматы и структуры данных.
    
    Args:
        row: Строка данных
        column_index: Индекс столбца с URL
        target_domain: Целевой домен для поиска в URL
        
    Returns:
        URL или пустая строка, если URL не найден
    """
    # Если target_domain не указан, используем глобальную константу
    if target_domain is None:
        target_domain = TARGET_DOMAIN
        
    # Проверяем аргументы
    if not row:
        return ""
        
    # Проверяем, достаточно ли длинная строка
    if len(row) <= column_index:
        logger.debug(f"Строка недостаточной длины для индекса {column_index}. Длина строки: {len(row)}")
        
        # Пытаемся найти URL в любом столбце
        for i, cell in enumerate(row):
            if isinstance(cell, str) and ('http' in cell.lower() or target_domain in cell.lower()):
                logger.debug(f"Найден URL в альтернативном столбце {i}: {cell}")
                return cell
                
        # Пытаемся найти URL в последнем столбце
        if len(row) > 0:
            last_cell = row[-1]
            if isinstance(last_cell, str) and ('http' in last_cell.lower() or target_domain in last_cell.lower()):
                logger.debug(f"Найден URL в последнем столбце: {last_cell}")
                return last_cell
                
        return ""
        
    # Получаем значение из указанного столбца
    url = row[column_index]
    
    # Если значение не строка (например, None или число), преобразуем в строку
    if not isinstance(url, str):
        url = str(url) if url is not None else ""
        
    # Удаляем лишние пробелы и другие символы
    url = url.strip()
    
    # Проверяем наличие URL-подобной строки
    if url and ('http://' in url.lower() or 'https://' in url.lower() or 'www.' in url.lower() or target_domain in url.lower()):
        return url
    elif url:
        # Если это строка, но не похожа на URL, проверяем подробнее
        logger.debug(f"Значение в столбце URL не похоже на URL: '{url}'")
        
    return url

def filter_domain_rows(rows, domain_column_index, target_domain, last_timestamp=None):
    """
    Фильтрует строки, содержащие заданный домен в указанном столбце
    и имеющие дату в первом столбце новее указанной.
    
    Args:
        rows: Список строк с данными
        domain_column_index: Индекс столбца с URL
        target_domain: Целевой домен для фильтрации
        last_timestamp: Пороговая дата (включительно) для фильтрации по времени
        
    Returns:
        Список отфильтрованных строк
    """
    filtered_rows = []
    total_processed = 0
    short_rows = 0
    domain_found = 0
    date_filtered = 0
    
    logger.info(f"Начинаем фильтрацию строк по домену {target_domain} в столбце с индексом {domain_column_index}")
    if last_timestamp:
        logger.info(f"Применяем фильтрацию по дате: строки с датой > {last_timestamp}")
    
    for i, row in enumerate(rows):
        try:
            total_processed += 1
            
            # Проверяем дату в первом столбце, если указана временная метка
            if last_timestamp and len(row) > 0:
                date_str = row[0]
                # Проверяем, что значение существует и выглядит как дата
                if not date_str or not isinstance(date_str, str):
                    logger.debug(f"Строка {i+1} не содержит дату в первом столбце: {row}")
                    continue
                    
                # Пробуем разобрать дату
                try:
                    row_date = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                    # Если дата старше или равна последней обработанной, пропускаем
                    if row_date <= last_timestamp:
                        date_filtered += 1
                        if i < 10 or i % 100 == 0:  # Для ограничения вывода
                            logger.debug(f"Строка {i+1} пропущена по дате: {row_date} <= {last_timestamp}")
                        continue
                except ValueError:
                    logger.debug(f"Не удалось разобрать дату '{date_str}' в строке {i+1}")
                    # Пропускаем строку, если не удалось разобрать дату
                    continue
            
            # Получаем URL из строки данных
            url = get_url_from_row(row, domain_column_index, target_domain)
            
            if not url:
                if len(row) > 0:  # Если строка не пустая, но URL отсутствует
                    short_rows += 1
                    if total_processed <= 5 or total_processed % 100 == 0:
                        logger.debug(f"Строка {i+1} не содержит URL в столбце {domain_column_index}: {row}")
                continue
                
            # Подробное логирование для отладки
            if i < 10 or i % 100 == 0:  # Логируем первые 10 строк и каждую сотую после
                logger.debug(f"Проверка строки {i+1}, URL: {url}")
                
            # Проверка наличия домена в URL
            if is_domain_in_url(url, target_domain):
                domain_found += 1
                logger.debug(f"Найден домен {target_domain} в строке {i+1}, URL: {url}")
                filtered_rows.append(row)
        except Exception as e:
            logger.error(f"Ошибка при обработке строки {i+1}: {e}")
            logger.debug(f"Содержимое строки: {row}")
    
    logger.info(f"Всего обработано строк: {total_processed}")
    logger.info(f"Строк без URL: {short_rows}")
    if last_timestamp:
        logger.info(f"Строк отфильтровано по дате (старые): {date_filtered}")
    logger.info(f"Строк с доменом {target_domain}: {domain_found}")
    
    if domain_found == 0:
        logger.warning(f"Не найдено ни одной строки с доменом {target_domain} в столбце с индексом {domain_column_index}")
    
    return filtered_rows

def get_or_create_target_sheet(service, spreadsheet_id):
    """
    Получает или создает вкладку для текущего месяца и года.
    
    Args:
        service: Сервис Google Sheets API
        spreadsheet_id: ID целевой таблицы
        
    Returns:
        Название вкладки
    """
    try:
        # Получаем текущий месяц и год
        now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
        
        # Для примера используем фиксированный месяц (согласно требованиям)
        # current_sheet_name = f"{RUSSIAN_MONTHS[now.month]} {now.year}"
        current_sheet_name = FIRST_MONTH
        
        logger.info(f"Проверяем наличие вкладки: '{current_sheet_name}'")
        
        # Получаем список существующих вкладок
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheet_exists = False
        
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['title'] == current_sheet_name:
                sheet_exists = True
                sheet_id = sheet['properties']['sheetId']
                logger.info(f"Вкладка '{current_sheet_name}' уже существует (ID вкладки: {sheet_id})")
                break
                
        # Создаем вкладку, если она не существует
        if not sheet_exists:
            request_body = {
                'requests': [{
                    'addSheet': {
                        'properties': {
                            'title': current_sheet_name
                        }
                    }
                }]
            }
            response = service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body=request_body
            ).execute()
            
            # Получаем ID созданной вкладки для логирования
            sheet_id = response['replies'][0]['addSheet']['properties']['sheetId']
            logger.info(f"Создана новая вкладка: '{current_sheet_name}' (ID вкладки: {sheet_id})")
            
        return current_sheet_name
    except Exception as e:
        logger.error(f"Ошибка при получении/создании целевой вкладки: {e}")
        raise

def get_existing_target_data(service, spreadsheet_id, sheet_name):
    """
    Получает существующие данные из целевой вкладки.
    
    Args:
        service: Сервис Google Sheets API
        spreadsheet_id: ID целевой таблицы
        sheet_name: Название вкладки
        
    Returns:
        Список существующих строк
    """
    try:
        result = service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}"
        ).execute()
        
        return result.get('values', [])
    except Exception as e:
        logger.error(f"Ошибка при получении существующих данных из целевой таблицы: {e}")
        # Если возникла ошибка, считаем что данных нет
        return []

def filter_duplicates(new_rows, existing_rows):
    """
    Исключает дубликаты из новых строк на основе существующих.
    
    Args:
        new_rows: Список новых строк для добавления
        existing_rows: Список существующих строк
        
    Returns:
        Список новых строк без дубликатов
    """
    # Преобразуем существующие строки в строковое представление для сравнения
    existing_str_rows = [','.join(str(cell) for cell in row) for row in existing_rows]
    
    unique_rows = []
    for row in new_rows:
        row_str = ','.join(str(cell) for cell in row)
        if row_str not in existing_str_rows:
            unique_rows.append(row)
            
    return unique_rows

def write_to_target_sheet(service, spreadsheet_id, sheet_name, rows):
    """
    Записывает данные в целевую таблицу и обновляет метаданные синхронизации.
    Новые строки добавляются в конец таблицы.
    
    Args:
        service: Сервис Google Sheets API
        spreadsheet_id: ID целевой таблицы
        sheet_name: Название вкладки
        rows: Строки для записи
        
    Returns:
        Количество записанных строк
    """
    try:
        if not rows:
            logger.info("Нет новых данных для записи")
            return 0
            
        logger.info(f"Записываем данные в целевую вкладку: {sheet_name}")
        
        # Выводим информацию о первой строке данных
        if rows:
            logger.info(f"Первая строка данных для переноса (после заголовка): {rows[0]}")
            
        # Записываем данные в конец таблицы
        body = {
            'values': rows
        }
        
        # Используем append вместо update для добавления строк в конец таблицы
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}",
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        # Обновляем метаданные в ячейке A1
        now = datetime.datetime.now(pytz.timezone('Europe/Moscow'))
        timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
        metadata = [
            [f"Последняя синхронизация: {timestamp}. Перенесено записей: {len(rows)}"]
        ]
        
        service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!A1",
            valueInputOption='RAW',
            body={'values': metadata}
        ).execute()
        
        logger.info(f"Успешно добавлено {len(rows)} строк в конец вкладки '{sheet_name}'")
        return len(rows)
    except Exception as e:
        logger.error(f"Ошибка при записи данных в целевую таблицу: {e}")
        raise

def check_spreadsheet_access(service, spreadsheet_id, description):
    """
    Проверяет доступ к таблице.
    
    Args:
        service: Сервис Google Sheets API
        spreadsheet_id: ID таблицы
        description: Описание таблицы для логирования
        
    Returns:
        True, если доступ есть, False - в противном случае
    """
    try:
        # Пытаемся получить метаданные таблицы
        spreadsheet = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        title = spreadsheet.get('properties', {}).get('title', 'Без названия')
        logger.info(f"Успешное подключение к таблице {description}: '{title}' (ID: {spreadsheet_id})")
        return True
    except Exception as e:
        logger.error(f"Ошибка доступа к таблице {description} (ID: {spreadsheet_id}): {e}")
        return False

def get_latest_timestamp_from_target(existing_data):
    """
    Получает последнюю (максимальную) дату из первого столбца целевой таблицы.
    
    Args:
        existing_data: Список строк данных из целевой таблицы
        
    Returns:
        datetime объект с последней датой или None, если даты не найдены
    """
    try:
        # Проверяем, что в таблице есть данные (минимум заголовок и одна строка)
        if len(existing_data) <= 1:
            logger.info("В целевой таблице нет данных для определения последней даты")
            return None
            
        # Пропускаем заголовок (первую строку)
        data_rows = existing_data[1:]
        
        latest_timestamp = None
        parsed_dates = []
        
        # Перебираем все строки и ищем максимальную дату
        for row in data_rows:
            if not row or len(row) == 0:
                continue
                
            # Получаем значение из первого столбца
            date_str = row[0]
            
            # Проверяем, что значение существует и выглядит как дата
            if not date_str or not isinstance(date_str, str):
                continue
                
            # Пробуем разобрать дату в формате "YYYY-MM-DD HH:MM:SS"
            try:
                dt = datetime.datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
                parsed_dates.append(dt)
            except ValueError:
                logger.debug(f"Не удалось разобрать дату '{date_str}' в строке {row}")
                continue
        
        # Если нашли хотя бы одну дату, определяем максимальную
        if parsed_dates:
            latest_timestamp = max(parsed_dates)
            logger.info(f"Найдена последняя дата в целевой таблице: {latest_timestamp}")
        else:
            logger.info("Не удалось найти корректные даты в целевой таблице")
            
        return latest_timestamp
    except Exception as e:
        logger.warning(f"Ошибка при определении последней даты: {e}")
        return None

def transfer_sheet_data(url_column_index=9, domain=None, source_sheet_name=None):
    """
    Основная функция для переноса данных между таблицами.
    
    Копирует строки с доменом forum-info.ru из исходной таблицы
    в целевую таблицу на соответствующей вкладке месяца и года.
    Учитывает только новые строки с датой новее последней в целевой таблице.
    
    Args:
        url_column_index: Индекс столбца с URL (по умолчанию 9 - столбец J)
        domain: Целевой домен для фильтрации (по умолчанию используется TARGET_DOMAIN)
        source_sheet_name: Название вкладки в исходной таблице
    """
    try:
        # Загружаем переменные окружения
        load_dotenv()
        
        # Используем переданный домен или глобальную константу
        target_domain = domain if domain else TARGET_DOMAIN
        
        logger.info(f"Запуск переноса данных. Используем столбец с индексом {url_column_index} для URL")
        logger.info(f"Целевой домен для фильтрации: {target_domain}")
        
        if source_sheet_name:
            logger.info(f"Будем искать данные во вкладке исходной таблицы: '{source_sheet_name}'")
        
        # Получаем ID таблиц
        source_spreadsheet_id = os.getenv('SPREADSHEET_ID_1')
        target_spreadsheet_id = os.getenv('SPREADSHEET_ID_2')
        
        if not source_spreadsheet_id or not target_spreadsheet_id:
            logger.error("ID таблиц не найдены в переменных окружения")
            return
            
        # Создаем сервис
        service = create_sheets_service()
        
        # Проверяем доступ к таблицам
        if not check_spreadsheet_access(service, source_spreadsheet_id, "источник"):
            return
            
        if not check_spreadsheet_access(service, target_spreadsheet_id, "назначение"):
            return
        
        # Получаем или создаем целевую вкладку
        target_sheet_name = get_or_create_target_sheet(service, target_spreadsheet_id)
        
        # Получаем существующие данные из целевой таблицы для проверки дубликатов и определения последней даты
        existing_data = get_existing_target_data(service, target_spreadsheet_id, target_sheet_name)
        
        if existing_data:
            logger.info(f"В целевой вкладке '{target_sheet_name}' уже есть {len(existing_data)} строк")
            
            # Выводим заголовок и первую строку данных целевой таблицы для отладки
            if len(existing_data) > 0:
                logger.info(f"Заголовок в целевой таблице: {existing_data[0] if existing_data[0] else 'Пусто'}")
                if len(existing_data) > 1:
                    logger.info(f"Первая строка данных в целевой таблице: {existing_data[1]}")
        
        # Получаем последнюю дату из целевой таблицы
        last_timestamp = None
        if existing_data:
            last_timestamp = get_latest_timestamp_from_target(existing_data)
            if last_timestamp:
                logger.info(f"Обрабатываем только записи новее: {last_timestamp}")
            else:
                logger.info("Не удалось определить последнюю дату, обрабатываем все записи")
        
        # Получаем данные из исходной таблицы, указывая нужный лист
        source_data = get_source_data(service, source_spreadsheet_id, source_sheet_name)
        
        if not source_data:
            logger.warning("Исходная таблица пуста или не содержит данных")
            return
            
        # Логируем структуру первых строк для отладки
        logger.info(f"Всего строк в исходной таблице: {len(source_data)}")
        header_row = source_data[0] if source_data else []
        logger.info(f"Заголовки таблицы: {header_row}")
        
        # Находим индекс столбца URL по заголовку
        if 'URL' in header_row:
            url_col_by_name = header_row.index('URL')
            logger.info(f"Найден заголовок 'URL' в столбце {url_col_by_name}")
            # Используем найденный индекс, если он отличается от указанного
            if url_col_by_name != url_column_index:
                logger.warning(f"Индекс столбца URL в заголовке ({url_col_by_name}) отличается от указанного ({url_column_index}). Используем {url_column_index}.")
        
        # Проверяем, что индекс URL не выходит за пределы количества столбцов
        if len(header_row) <= url_column_index:
            logger.warning(f"Указанный индекс столбца URL ({url_column_index}) превышает количество столбцов в таблице ({len(header_row)})")
            logger.info(f"Используем последний доступный столбец: {len(header_row)-1}")
            url_column_index = len(header_row) - 1
        
        # Пропускаем заголовок (первую строку)
        data_rows = source_data[1:] if len(source_data) > 0 else []
        logger.info(f"Обрабатываем {len(data_rows)} строк данных (после пропуска заголовка)")
        
        # Логируем примеры URL для отладки
        sample_urls = []
        for i, row in enumerate(data_rows[:10]):  # Проверяем первые 10 строк
            if len(row) > url_column_index:
                url = row[url_column_index]
                sample_urls.append(f"[{i+1}] {url}")
            else:
                sample_urls.append(f"[{i+1}] <URL отсутствует>")
                
        logger.info(f"Примеры URL из таблицы (первые 10 строк):")
        for sample in sample_urls:
            logger.info(f"  {sample}")
        
        # Используем более продвинутую фильтрацию строк с доменом и учетом даты
        filtered_rows = filter_domain_rows(data_rows, url_column_index, target_domain, last_timestamp)
        
        if not filtered_rows:
            logger.info(f"Не найдено новых строк с доменом {target_domain}")
            return
        
        # Фильтруем дубликаты
        unique_rows = filter_duplicates(filtered_rows, existing_data)
        
        if not unique_rows:
            logger.info("Все найденные строки уже существуют в целевой таблице")
            return
        
        logger.info(f"Найдено {len(unique_rows)} уникальных строк для переноса")
            
        # Записываем данные в целевую таблицу
        rows_written = write_to_target_sheet(service, target_spreadsheet_id, target_sheet_name, unique_rows)
        
        logger.info(f"Успешно перенесено {rows_written} строк в таблицу {target_spreadsheet_id} на вкладку {target_sheet_name}")
        
    except Exception as e:
        logger.error(f"Произошла ошибка при переносе данных: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    try:
        # Вывод информации о среде выполнения
        logger.info(f"Версия Python: {sys.version}")
        logger.info(f"Текущий рабочий каталог: {os.getcwd()}")
        
        # Парсинг аргументов командной строки
        parser = argparse.ArgumentParser(description='Перенос данных между Google таблицами.')
        parser.add_argument('--column', type=int, default=9, 
                            help='Индекс столбца с URL (по умолчанию 9, что соответствует столбцу J)')
        parser.add_argument('--debug', action='store_true', 
                            help='Включить отладочный режим логирования')
        parser.add_argument('--domain', type=str, default=TARGET_DOMAIN,
                            help=f'Целевой домен для фильтрации (по умолчанию {TARGET_DOMAIN})')
        parser.add_argument('--source_sheet', type=str, default=FIRST_MONTH,
                            help=f'Название вкладки в исходной таблице (по умолчанию {FIRST_MONTH})')
        
        args = parser.parse_args()
        
        # Настройка уровня логирования
        if args.debug:
            set_debug_logging()
            
        # Используем локальный параметр domain вместо изменения глобальной переменной
        domain_to_use = args.domain
        if domain_to_use != TARGET_DOMAIN:
            logger.info(f"Используем указанный домен: {domain_to_use} вместо стандартного {TARGET_DOMAIN}")
            
        # Запуск основной функции с указанием домена и вкладки
        transfer_sheet_data(url_column_index=args.column, domain=domain_to_use, source_sheet_name=args.source_sheet)
    except Exception as e:
        logger.critical(f"Критическая ошибка при выполнении скрипта: {e}", exc_info=True)
        sys.exit(1) 