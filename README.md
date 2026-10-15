# Микросервисная система управления промышленным оборудованием

Kafka, PostgreSQL, Spring Security, JWT, API Gateway на edge-узле (промышленный станок).

---

## 📦 Архитектура

```
    📱 Мобильное приложение (основной клиент)      🖥️ Веб-админка
                  │                                        │
                  └──────────────┬─────────────────────────┘
                                 ▼
                    API Gateway (8080)
                    Spring Cloud Gateway
                                 │
                ┌────────────────┴────────────────┐
                ▼                                 ▼
        user-service (8081)              program-service (8082)
        - Spring Security                - CRUD REST API
        - JWT (BCrypt)                   - Kafka Producer
        - Kafka Consumer                 - PostgreSQL (program_db)
        - PostgreSQL (userdb)
                │                                 │
                └────────────┬────────────────────┘
                             ▼
                    Apache Kafka (9092)
                    топик: program-events
```

**Всё развёрнуто на промышленном компьютере станка под Ubuntu.** Никакого внешнего сервера, никакого облака.

---

## Почему edge-архитектура (on-premise)

Это **edge computing** — вычисления на периферии, рядом с оборудованием, а не в центральном дата-центре.

**Почему так, а не в облаке:**

| Причина | Что даёт |
|---------|----------|
| **Интернет в цеху нестабилен** | Система работает автономно, без зависимости от внешней сети |
| **Мгновенная реакция** | Оператор меняет настройки — станок реагирует за миллисекунды, без задержки на сеть |
| **Безопасность данных** | Данные не покидают цех — требование ИБ на промышленных объектах |
| **Отказоустойчивость** | Упал интернет — система продолжает работать |
| **Простота эксплуатации** | Заказчик сам контролирует сервер, не платит за облако |

**Минусы, о которых знаем:**
- Дороже на старте (нужен промышленный ПК под Ubuntu)
- Обновления ставить сложнее (надо ходить на каждый станок)
- Масштабирование требует парка таких edge-узлов

**Для одного станка формально хватило бы монолита**, но мы заложили микросервисную архитектуру, потому что:
- Есть потенциал развернуть систему на парке станков
- Разделение ответственности: user-service не знает про программы гибки
- Kafka развязывает сервисы асинхронно
- Можно добавить ещё сервисы (например, notification-service), не трогая существующие

---

## Роль каждого сервиса

### API Gateway (8080)
- Единая точка входа для мобильного приложения и веб-админки
- Маршрутизация:
    - `/auth/**` и `/api/users/**` → user-service (8081)
    - `/api/programs/**` → program-service (8082)
- Работает на реактивном стеке Spring WebFlux + Netty

### user-service (8081)
- Регистрация и логин пользователей
- JWT-аутентификация (Spring Security + BCrypt)
- Kafka Consumer — слушает `program-events`, пишет аудит действий оператора
- Хранит пользователей в PostgreSQL (`userdb`, таблица `users`)

### program-service (8082)
- CRUD REST API для программ гибки
- Kafka Producer — при создании/изменении программы отправляет событие в `program-events`
- Хранит программы в PostgreSQL (`program_db`, таблица `programs`)

---

## Сценарий: оператор создаёт программу гибки

1. Оператор в мобильном приложении заполняет программу
2. Запрос `POST /api/programs` идёт в API Gateway (8080)
3. Gateway маршрутизирует в program-service (8082)
4. program-service:
    - сохраняет программу в PostgreSQL
    - отправляет событие `PROGRAM_CREATED` в Kafka (`program-events`)
    - возвращает клиенту созданную программу
5. user-service (Consumer):
    - слушает `program-events`
    - получает событие
    - записывает в аудит: «оператор X создал программу Y в 12:34»
6. Ответ клиенту возвращается через Gateway

** обмен асинхронный.

---

## Стек технологий

| Технология | Назначение |
|------------|-----------|
| Java 17 | Язык разработки |
| Spring Boot 3.4.0 | Фреймворк |
| Spring Cloud Gateway | API Gateway |
| Spring Security + JWT (jjwt 0.11.5) | Аутентификация |
| Spring Data JPA + Hibernate | Работа с БД |
| PostgreSQL 16 | Основная БД |
| Apache Kafka 4.x (KRaft) | Асинхронный обмен событиями |
| Spring Boot Actuator + Micrometer | Мониторинг и метрики |
| Maven | Сборка |
| Ubuntu 24.04 | Среда разработки (on-premise) |
| Git + GitHub | Версионирование |

---

## Установка и запуск

### Шаг 1. Скрипт автоматической установки (Ubuntu)

Сохрани в файл `setup-all.sh` и выполни:

```bash
chmod +x setup-all.sh
./setup-all.sh
```

**Что делает скрипт (полный код ниже):**
- Устанавливает Java 17, PostgreSQL 16
- Создаёт пользователя `appuser` и базы `userdb`, `program_db`
- Скачивает Kafka, форматирует хранилище (KRaft), запускает
- Создаёт топик `program-events`

### Полный код `setup-all.sh`

```bash
#!/bin/bash
# Автоматическая установка инфраструктуры для микросервисной системы
# Ubuntu 24.04 LTS
# Использование: chmod +x setup-all.sh && ./setup-all.sh

set -e

echo "======================================"
echo " Установка инфраструктуры (edge-узел)"
echo "======================================"

# ==========================================
# 1. Java 17
# ==========================================
echo ""
echo "[1/6] Установка Java 17..."
sudo apt update
sudo apt install -y openjdk-17-jdk

java -version

# ==========================================
# 2. PostgreSQL 16
# ==========================================
echo ""
echo "[2/6] Установка PostgreSQL 16..."
sudo apt install -y postgresql postgresql-contrib

sudo systemctl enable postgresql
sudo systemctl start postgresql

# ==========================================
# 3. Создание пользователя и баз
# ==========================================
echo ""
echo "[3/6] Создание пользователя и баз данных..."

sudo -u postgres psql <<EOF
DO \$\$
BEGIN
   IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'appuser') THEN
      CREATE USER appuser WITH PASSWORD 'secret';
   END IF;
END
\$\$;
EOF

sudo -u postgres psql -c "CREATE DATABASE userdb OWNER appuser;" 2>/dev/null || echo "userdb уже существует"
sudo -u postgres psql -c "CREATE DATABASE program_db OWNER appuser;" 2>/dev/null || echo "program_db уже существует"

sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE userdb TO appuser;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE program_db TO appuser;"

echo "Базы созданы: userdb, program_db (user: appuser, password: secret)"

# ==========================================
# 4. Kafka (KRaft, без Zookeeper)
# ==========================================
echo ""
echo "[4/6] Установка Kafka..."

KAFKA_VERSION="3.7.0"
KAFKA_DIR="/opt/kafka"

if [ ! -d "$KAFKA_DIR" ]; then
    cd /opt
    sudo wget -q "https://downloads.apache.org/kafka/${KAFKA_VERSION}/kafka_2.13-${KAFKA_VERSION}.tgz"
    sudo tar -xzf "kafka_2.13-${KAFKA_VERSION}.tgz"
    sudo mv "kafka_2.13-${KAFKA_VERSION}" kafka
    sudo chown -R $USER:$USER /opt/kafka
fi

cd /opt/kafka

# ==========================================
# 5. Форматирование и запуск Kafka
# ==========================================
echo ""
echo "[5/6] Форматирование и запуск Kafka..."

# Очистка старого хранилища (если есть)
if [ -d "/tmp/kraft-combined-logs" ]; then
    sudo rm -rf /tmp/kraft-combined-logs
fi

KAFKA_CLUSTER_ID=$(bin/kafka-storage.sh random-uuid)
echo "Cluster ID: $KAFKA_CLUSTER_ID"

bin/kafka-storage.sh format -t "$KAFKA_CLUSTER_ID" \
    -c config/server.properties --standalone

# Запуск Kafka в фоне
nohup bin/kafka-server-start.sh config/server.properties > /tmp/kafka.log 2>&1 &
echo "Kafka запускается в фоне (лог: /tmp/kafka.log)"

# Ждём запуска
sleep 20

# ==========================================
# 6. Создание топика
# ==========================================
echo ""
echo "[6/6] Создание топика program-events..."

bin/kafka-topics.sh --create \
    --topic program-events \
    --bootstrap-server localhost:9092 \
    --partitions 1 \
    --replication-factor 1 2>/dev/null || echo "Топик уже существует"

echo ""
echo "======================================"
echo "Установка завершена!"
echo "======================================"
echo ""
echo "PostgreSQL: localhost:5432"
echo "  - БД: userdb, program_db"
echo "  - Пользователь: appuser"
echo "  - Пароль: secret"
echo ""
echo "Kafka: localhost:9092"
echo "  - Топик: program-events"
echo "  - Лог: /tmp/kafka.log"
echo ""
echo "Проверка Kafka:"
echo "  /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092"
echo ""
```

### Шаг 2. Сборка JAR-ников

На Windows или Ubuntu (где есть Maven):

```bash
# В папке каждого проекта
cd user-service
mvn clean package -DskipTests

cd ../program-service
mvn clean package -DskipTests

cd ../api-gateway
mvn clean package -DskipTests
```

На выходе — три JAR-файла в папках `target/`.

### Шаг 3. Запуск сервисов на Ubuntu

Скопируй три JAR-ника на Ubuntu и запусти в отдельных терминалах:

```bash
# Терминал 1
java -jar user-service-0.0.1-SNAPSHOT.jar

# Терминал 2
java -jar program-service-0.0.1-SNAPSHOT.jar

# Терминал 3
java -jar api-gateway-0.0.1-SNAPSHOT.jar
```

### Шаг 4. Проверка

```bash
# Health-check Gateway
curl http://localhost:8080/actuator/health

# Регистрация
curl -X POST http://localhost:8080/auth/register \
  -H "Content-Type: application/json" \
  -d '{"username":"ermek","password":"secret","role":"USER"}'

# Логин → JWT
curl -X POST http://localhost:8080/auth/login \
  -H "Content-Type: application/json" \
  -d '{"username":"ermek","password":"secret"}'

# Создание программы
curl -X POST http://localhost:8080/api/programs \
  -H "Content-Type: application/json" \
  -d '{"name":"Test","metalType":"Steel","thickness":2.0,"createdBy":"ermek"}'

# Список программ
curl http://localhost:8080/api/programs
```

---

## Репозитории

- [user-service](https://github.com/C00lness/user-service) — пользователи, JWT, Kafka Consumer
- [program-service](https://github.com/C00lness/program-service) — программы гибки, Kafka Producer
- [api-gateway](https://github.com/C00lness/api-gateway) — единая точка входа

---

## 🎓 Что показывает проект

- Микросервисная архитектура (3 сервиса)
- Асинхронный обмен через Kafka
- Spring Security + JWT (BCrypt, фильтры, SecurityContext)
- Работа с PostgreSQL через JPA/Hibernate
- Единая точка входа (Spring Cloud Gateway)
- Мониторинг (Actuator + Micrometer)
- Развёртывание на Ubuntu (on-premise, edge computing)
- Автоматизация установки через bash
- Работа с Git, CI/CD (заготовки)

---

## Roadmap

- [x] user-service: Spring Security + JWT
- [x] program-service: CRUD + Kafka Producer
- [x] user-service: Kafka Consumer
- [x] API Gateway
- [x] setup-all.sh
- [ ] Запись аудита в БД (таблица `audit_log`)
- [ ] Prometheus + Grafana
- [ ] GitHub Actions (CI)
- [ ] Testcontainers (интеграционные тесты)
- [ ] systemd-службы для сервисов