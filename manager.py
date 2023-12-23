
import sqlite3
# from .errors import ColumnNotFoundError

class Connect:
	def __init__(self, path, table):
		self.table = table
		self.db_instance = sqlite3.connect(path)

	def select(self, cols: list = [], cond = [], aggr = None, fetchall = False):
		"""
		Выполняет SELECT-запрос к базе данных SQLite.

		:param table: Имя таблицы, к которой выполняется запрос.
		:param cols: Список столбцов для выборки (по умолчанию все столбцы).
		:param cond: Список условий для оператора WHERE.
		:param aggr: Агрегирующая функция (например, 'COUNT', 'SUM' и т. д.) или None для выборки столбцов.
		:param fetchall: Если True, возвращает все строки, иначе возвращает первую строку (по умолчанию False).
		:return: Результат запроса (все строки или первая строка, в зависимости от fetchall).
		"""
		with self.db_instance as con:
			cur = con.cursor()
			columns_str = ', '.join(cols) if cols else '*'
			aggregation_str = f"{aggr}" if aggr else columns_str
			cond = f"WHERE {'and '.join(cond)}" if cond else ""
			query = f"SELECT {aggregation_str} FROM {self.table} {cond}"
			if fetchall:
				return cur.execute(query).fetchall()

			return cur.execute(query).fetchone()


	def insert(self, values, on_conflict=None, return_id=False):
		"""
		Выполняет INSERT-запрос к базе данных SQLite.

		:param table: Имя таблицы, в которую выполняется вставка.
		:param values: Словарь значений, где ключи - столбцы, а значения - значения для вставки.
		:param on_conflict: Опция конфликта (например, 'IGNORE', 'REPLACE' и т. д.) или None (по умолчанию).
		:param return_id: Если True, возвращает идентификатор вставленной строки (по умолчанию False).
		:return: Возвращает идентификатор вставленной строки, если return_id=True, иначе возвращает None.
		"""
		with self.db_instance as con:
			cur = con.cursor()

			# Формирование SQL-запроса
			columns_str = ', '.join(values.keys())
			values_str = ', '.join(['?' for _ in range(len(values))])
			query = f"INSERT INTO {self.table} ({columns_str}) VALUES ({values_str})"

			# Добавление опции конфликта, если указана
			if on_conflict:
				query += f" ON CONFLICT {on_conflict}"

			# Выполнение запроса
			cur.execute(query, list(values.values()))

			# Если указан return_id, возвращаем идентификатор вставленной строки
			if return_id:
				return cur.lastrowid
			else:
				return None

	def update(self, values, cond, on_conflict=None):
	    """
	    Выполняет UPDATE-запрос к базе данных SQLite.

	    :param table: Имя таблицы, в которой выполняется обновление.
	    :param values: Словарь значений для обновления, где ключи - столбцы, а значения - новые значения.
	    :param cond: Список условий для оператора WHERE.
	    :param on_conflict: Опция конфликта (например, 'IGNORE', 'REPLACE' и т. д.) или None (по умолчанию).
	    :return: Количество обновленных строк.
	    """
	    with self.db_instance as con:
	        cur = con.cursor()

	        # Формирование SQL-запроса
	        set_clause = ', '.join([f"{col} = ?" for col in values.keys()])
	        cond_str = ' AND '.join(cond) if cond else ''
	        query = f"UPDATE {self.table} SET {set_clause} WHERE {cond_str}"

	        # Добавление опции конфликта, если указана
	        if on_conflict:
	            query += f" ON CONFLICT {on_conflict}"

	        # Выполнение запроса и возврат количества обновленных строк
	        return cur.execute(query, list(values.values())).rowcount
	        

	def delete(self, cond, on_conflict=None):
	    """
	    Выполняет DELETE-запрос к базе данных SQLite.

	    :param cond: Список условий для оператора WHERE.
	    :param on_conflict: Опция конфликта (например, 'IGNORE', 'REPLACE' и т. д.) или None (по умолчанию).
	    :return: Количество удаленных строк.
	    """
	    with self.db_instance as con:
	        cur = con.cursor()

	        # Формирование SQL-запроса
	        cond_str = ' AND '.join(cond) if cond else ''
	        query = f"DELETE FROM {self.table} WHERE {cond_str}"

	        # Добавление опции конфликта, если указана
	        if on_conflict:
	            query += f" ON CONFLICT {on_conflict}"

	        # Выполнение запроса и возврат количества удаленных строк
	        return cur.execute(query).rowcount
