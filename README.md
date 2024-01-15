[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://github.com/ydb-platform/ydb-logstash-topics-plugin/blob/main/LICENSE)

# LogStash plugin for [YDB Topics](https://ydb.tech/en/docs/concepts/topic).


## Content
1. [Input Plugin](#input)
2. [Output Plugin](#output)

<a name="input"></a>
## Input Plugin

На вход плагина поступают данные из **YDB Topics**, а на выход передаются непосредтсвенно в **Logstash**.

### Инструкция по развертыванию плагина на локальной машине

#### 1. Подготовка
Скачать Logstash Codebase по [ссылке](https://disk.yandex.ru/d/nnfPnenQhdP8yw) 
#### 2. Добавление файла в проект
В корне проекта создать файл *gradle.properties* и установить параметр **LOGSTASH_CORE_PATH=D:/Lib/logstash-main/logstash-core**
#### 3. Подготовка окружения
Для запуска и проверки работоспособности плагина потребуется запустить [YDB](https://ydb.tech/ru/docs/getting_started/self_hosted/ydb_docker)
#### 4. Инстуркция по установке плагина 
Для установки плагина в Logstash нужно: 
- Собрать проект командной 
  - ```./gradlew gem ``` на Linux системах
  - ```./gradlew.bat ``` на Windows системах
- Установить плагин соотвествующей командой: ```bin/logstash-plugin install --no-verify --local /path/to/javaPlugin.gem```
- Использовать тестовую конфигурацию для запуска плагина командой ```bin/logstash -f /path/to/java_input.conf```
### Тестовая Конфигурация Input Плагина (анонимная аутентификация)

```
input {
  ydb_topics_input {
    count => 3  
    prefix => "message"  
    topic_path => "topic_path"  
    connection_string => "grpc://localhost:2136?database=/local"
    consumer_name => "consumer_name"
    schema => "JSON"
  }
}

output {
  stdout { codec => rubydebug }  # Вывод в стандартный вывод с форматированием Ruby Debug
}
```

### Тестовая Конфигурация Input Плагина (аутентификация по токену)

```
input {
  ydb_topics_input {
    count => 3  
    prefix => "message"  
    topic_path => "topic_path"  
    connection_string => "grpc://localhost:2136?database=/local"
    consumer_name => "consumer_name"
    access_token => "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOi"
    schema => "JSON"
  }
}

output {
  stdout { codec => rubydebug }  # Вывод в стандартный вывод с форматированием Ruby Debug
}
```

### Тестовая Конфигурация Input Плагина (аутентификация при помощи файла сервисного аккаунта)

```
input {
  ydb_topics_input {
    count => 3  
    prefix => "message"  
    topic_path => "topic_path"  
    connection_string => "grpc://localhost:2136?database=/local"
    consumer_name => "consumer_name"
    service_account_key => "path/to/sa_file.json"
    schema => "JSON"
  }
}
```


<a name="output"></a>
## Output Plugin

На вход плагина поступают данные из **Logstash** , а на выход передаются непосредственно в **YDB Topics**.


### Инструкция по развертыванию плагина на локальной машине

#### 1. Подготовка
Скачать Logstash Codebase по [ссылке](https://disk.yandex.ru/d/nnfPnenQhdP8yw)
#### 2. Добавление файла в проект
В корне проекта создать файл *gradle.properties* и установить параметр **LOGSTASH_CORE_PATH=D:/Lib/logstash-main/logstash-core**
#### 3. Подготовка окружения
Для запуска и проверки работоспособности плагина потребуется запустить [YDB](https://ydb.tech/ru/docs/getting_started/self_hosted/ydb_docker)
#### 4. Инструкция по установке плагина
Для установки плагина в Logstash нужно:
- Собрать проект командной
    - ```./gradlew gem ``` на Linux системах
    - ```./gradlew.bat ``` на Windows системах
- Установить плагин этой командой: ```bin/logstash-plugin install --no-verify --local /path/to/javaPlugin.gem```
- Использовать тестовую конфигурацию для запуска плагина командой ```bin/logstash -f /path/to/java_output.conf```
### Тестовая Конфигурация Output Плагина (анонимная аутентификация)

```
input {
  stdin {
    codec => line
  }
}

output {
  ydb_topics_output {
    count => 3  
    prefix => "message" 
    topic_path => "topic_path"  
    producer_id => "test_producer"
    connection_string => "grpc://localhost:2136?database=/local"
  }
}
```
### Тестовая Конфигурация Output Плагина (аутентификация по токену)

```
input {
  stdin {
    codec => line
  }
}

output {
  ydb_topics_output {
    count => 3  
    prefix => "message" 
    topic_path => "topic_path"  
    producer_id => "test_producer"
    connection_string => "grpc://localhost:2136?database=/local"
    access_token => "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOi"
  }
}
```

### Тестовая Конфигурация Output Плагина (аутентификация при помощи файла сервисного аккаунта)

```
input {
  stdin {
    codec => line
  }
}

output {
  ydb_topics_output {
    count => 3  
    prefix => "message" 
    topic_path => "topic_path"  
    producer_id => "test_producer"
    connection_string => "grpc://localhost:2136?database=/local"
    service_account_key => "path/to/sa_file.json"
  }
}
```

