import sqlite3
import pandas as pd
import json
import statistics

# Параметр VAR
VAR = 5  # Измените это значение при необходимости

# Шаг 1. Считать данные из файла
input_file = "item.csv"  # Замените на ваш файл
data = pd.read_csv(input_file)

# Шаг 2. Создать базу данных и таблицу
db_name = "database.db"
table_name = "data_table"

conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# Создание таблицы
columns = data.columns
types = []

for col in data.columns:
    if data[col].dtype == "int64":
        types.append("INTEGER")
    elif data[col].dtype == "float64":
        types.append("REAL")
    else:
        types.append("TEXT")

columns_with_types = ", ".join([f"{col} {typ}" for col, typ in zip(columns, types)])
cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types});")
conn.commit()

# Вставка данных в таблицу
data.to_sql(table_name, conn, if_exists="replace", index=False)

# Шаг 3. Запросы
# 1. Вывод первых (VAR+10) отсортированных по числовому полю строк в JSON
numeric_field = "numeric_column"  # Замените на ваше числовое поле
query = f"SELECT * FROM {table_name} ORDER BY {numeric_field} LIMIT {VAR + 10};"
result = cursor.execute(query).fetchall()

with open("sorted_data.json", "w") as json_file:
    json.dump(result, json_file, indent=4)

# 2. Вывод статистики по числовому полю
numeric_data = data[numeric_field]
stats = {
    "sum": numeric_data.sum(),
    "min": numeric_data.min(),
    "max": numeric_data.max(),
    "average": numeric_data.mean(),
}

print("Statistics:", stats)

# 3. Вывод частоты встречаемости для категориального поля
categorical_field = "category_column"  # Замените на ваше категориальное поле
frequency = data[categorical_field].value_counts().to_dict()

print("Frequency:", frequency)

# 4. Отфильтрованные данные
predicate = "numeric_column > 50"  # Замените на ваш предикат
query_filtered = f"""
SELECT * FROM {table_name}
WHERE {predicate}
ORDER BY {numeric_field}
LIMIT {VAR + 10};
"""
filtered_result = cursor.execute(query_filtered).fetchall()

with open("filtered_sorted_data.json", "w") as json_file:
    json.dump(filtered_result, json_file, indent=4)

# Закрытие соединения
conn.close()
