# Скрипт переноса данных между Google таблицами

Автоматизированное решение для переноса строк содержащих определенный домен из одной Google таблицы в другую.

## Назначение и функциональность

Скрипт предназначен для выполнения следующих задач:

1. Подключение к Google Sheets API с использованием учетных данных сервисного аккаунта
2. Чтение данных из указанной вкладки исходной таблицы
3. Фильтрация строк, содержащих указанный домен (по умолчанию forum-info.ru) в URL
4. Фильтрация строк по дате - обработка только тех записей, которые новее последней записи в целевой таблице
5. Создание или выбор целевой вкладки с названием "<месяц> <год>" в таблице назначения
6. Запись отфильтрованных строк в целевую таблицу, исключая дубликаты
7. Обновление метаданных о синхронизации в целевой таблице

## Требования и зависимости

- Python 3.6 или выше
- Библиотеки Python (установка через `pip install -r requirements.txt`):
  - google-auth
  - google-auth-oauthlib
  - google-auth-httplib2
  - google-api-python-client
  - python-dotenv
  - pytz
  - psutil
  - schedule (для планировщика)

## Настройка проекта

### 1. Создание учетных данных Google API

1. Откройте [Google Cloud Console](https://console.cloud.google.com/)
2. Создайте проект или выберите существующий
3. Включите Google Sheets API
4. Создайте сервисный аккаунт и скачайте ключ JSON
5. Поместите файл ключа в каталог `credentials/`

### 2. Настройка файла .env

Создайте файл `.env` в корневом каталоге проекта со следующими параметрами:

```
# Google Sheets credentials
GOOGLE_CREDENTIALS_FILE=путь/к/файлу/credentials/ключ.json

# Google Sheets IDs
SPREADSHEET_ID_1=ID_исходной_таблицы
SPREADSHEET_ID_2=ID_целевой_таблицы
```

Где:
- `GOOGLE_CREDENTIALS_FILE` - полный путь к файлу учетных данных сервисного аккаунта
- `SPREADSHEET_ID_1` - ID исходной Google таблицы (часть URL после `/d/` и перед `/edit`)
- `SPREADSHEET_ID_2` - ID целевой Google таблицы

### 3. Настройка доступа к таблицам

Предоставьте сервисному аккаунту доступ к обеим таблицам, используя функцию "Настройка доступа" в Google Sheets и добавив email сервисного аккаунта с правами редактора.

## Структура проекта

```
SheetTransferScript/
├── sheet_transfer.py     # Основной скрипт переноса данных
├── scheduler.py          # Планировщик регулярного запуска
├── requirements.txt      # Зависимости
├── .env                  # Конфигурационный файл
├── credentials/          # Каталог с учетными данными
│   └── ключ.json         # Ключ сервисного аккаунта Google
├── venv/                 # Виртуальное окружение Python
└── README.md             # Документация проекта
```

## Параметры командной строки

Скрипт поддерживает следующие параметры:

- `--column` - Индекс столбца с URL (по умолчанию 9, что соответствует столбцу J)
- `--debug` - Включает режим отладки с подробным логированием
- `--domain` - Изменяет целевой домен для фильтрации (по умолчанию forum-info.ru)
- `--source_sheet` - Указывает название вкладки в исходной таблице (по умолчанию "Май 2025")

### Примеры запуска:

```bash
# Базовый запуск с параметрами по умолчанию
python sheet_transfer.py

# Изменение индекса столбца URL на 10 (столбец K)
python sheet_transfer.py --column 10

# Включение режима отладки
python sheet_transfer.py --debug

# Изменение целевого домена
python sheet_transfer.py --domain example.com

# Указание конкретной вкладки в исходной таблице
python sheet_transfer.py --source_sheet "Июнь 2025"

# Комбинация параметров
python sheet_transfer.py --column 10 --domain example.com --source_sheet "Июнь 2025" --debug
```

## Планировщик запуска

Проект включает планировщик `scheduler.py`, который автоматически запускает основной скрипт с регулярными интервалами.

### Особенности планировщика:

- Запускает скрипт `sheet_transfer.py` с заданным интервалом (по умолчанию каждый час)
- Выполняет первый запуск сразу после старта
- Отображает логи основного скрипта в консоли
- Корректно обрабатывает сигналы завершения (Ctrl+C)

### Запуск планировщика:

```bash
python scheduler.py
```

### Настройка интервала запуска:

Для изменения интервала запуска отредактируйте константу `INTERVAL_SECONDS` в файле `scheduler.py`. По умолчанию установлен интервал в 3600 секунд (1 час).

## Логирование и отладка

Скрипт создает подробные логи выполнения в:
- Консоль (стандартный вывод)

При включении параметра `--debug` логирование становится более подробным, включая:
- Информацию о проверке URL
- Данные о проверке наличия домена в каждой строке
- Подробную информацию о работе с API
- Детальную информацию о фильтрации по дате

## Особенности работы скрипта

### Определение URL и доменов

Скрипт проверяет наличие целевого домена в URL с учетом различных форматов записи:
- Учитываются протоколы (http://, https://)
- Распознаются поддомены (subdomain.forum-info.ru)
- Обнаруживаются домены в различных частях URL

### Работа с вкладками

1. В исходной таблице:
   - Указывается конкретная вкладка с помощью `--source_sheet`
   - Если вкладка не найдена, скрипт выводит список доступных вкладок

2. В целевой таблице:
   - Создается или используется вкладка с названием, соответствующим текущему месяцу и году
   - По умолчанию используется вкладка "Май 2025" (задается константой FIRST_MONTH)

### Фильтрация по дате и защита от дубликатов

1. **Фильтрация по времени добавления:**
   - Скрипт определяет последнюю (максимальную) дату в первом столбце целевой таблицы
   - Обрабатывает только те строки из исходной таблицы, которые содержат более позднюю дату
   - Это значительно ускоряет обработку при больших объемах данных и сокращает API-запросы

2. **Защита от дубликатов:**
   - Дополнительно сравниваются строковые представления строк для исключения дубликатов
   - Переносятся только полностью уникальные строки

3. **Добавление данных:**
   - Новые строки добавляются в конец целевой таблицы, не затирая существующие данные
   - В ячейке A1 обновляется метаинформация о последней синхронизации

## Примеры использования

### Базовый сценарий:

```bash
python sheet_transfer.py
```

Скрипт:
1. Подключится к указанным в .env таблицам
2. Прочитает данные из вкладки "Май 2025" в исходной таблице
3. Определит последнюю дату в целевой таблице
4. Найдет строки с более новыми датами, содержащие домен forum-info.ru в столбце J (индекс 9)
5. Создаст вкладку "Май 2025" в целевой таблице, если она не существует
6. Добавит найденные строки в конец целевой таблицы
7. Запишет метаданные о синхронизации в первую строку целевой таблицы

### Автоматический запуск по расписанию:

```bash
python scheduler.py
```

Планировщик:
1. Запустит скрипт переноса данных немедленно
2. Будет повторять запуск каждый час
3. Отобразит все логи выполнения скрипта
4. Будет работать до получения сигнала завершения (Ctrl+C)

### Сценарий с изменением параметров:

```bash
python sheet_transfer.py --column 8 --source_sheet "Июнь 2025" --debug
```

Скрипт:
1. Включит подробное логирование (режим отладки)
2. Подключится к указанным таблицам
3. Прочитает данные из вкладки "Июнь 2025" в исходной таблице
4. Определит последнюю дату в целевой таблице
5. Найдет строки с более новыми датами, содержащие домен forum-info.ru в столбце I (индекс 8)
6. Создаст вкладку "Май 2025" в целевой таблице и добавит найденные строки
7. Сохранит подробный лог выполнения 