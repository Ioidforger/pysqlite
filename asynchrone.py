import aiosqlite

class AsyncConnect:
    def __init__(self, path, table):
        self.table = table
        self.db_instance = None
        self.path = path

    async def connect(self):
        self.db_instance = await aiosqlite.connect(self.path)

    async def select(self, cols: list = [], cond=None, aggr=None, fetchall=False):
        if self.db_instance is None:
            await self.connect()

        columns_str = ', '.join(cols) if cols else '*'
        cond_str = ' AND '.join(cond) if isinstance(cond, list) else cond or ''
        aggregation_str = f"{aggr}" if aggr else columns_str
        query = f"SELECT {aggregation_str} FROM {self.table} WHERE {cond_str}"

        async with self.db_instance.execute(query) as cur:
            if fetchall:
                return await cur.fetchall()
            return await cur.fetchone()

    async def insert(self, values=None, on_conflict=None, return_id=True):
        if self.db_instance is None:
            await self.connect()

        columns_str = ', '.join(values.keys())
        values_str = ', '.join(['?' for _ in range(len(values))])
        query = f"INSERT INTO {self.table} ({columns_str}) VALUES ({values_str})"

        if on_conflict:
            query += f" ON CONFLICT {on_conflict}"

        async with self.db_instance.execute(query, list(values.values())) as cur:
            if return_id:
                return cur.lastrowid
            else:
                return None

    async def update(self, values, cond=None, on_conflict=None):
        if self.db_instance is None:
            await self.connect()

        set_clause = ', '.join([f"{col} = ?" for col in values.keys()])
        cond_str = ' AND '.join(cond) if cond and isinstance(cond, list) else cond or ''
        query = f"UPDATE {self.table} SET {set_clause} WHERE {cond_str}"

        if on_conflict:
            query += f" ON CONFLICT {on_conflict}"

        async with self.db_instance.execute(query, list(values.values())) as cur:
            return cur.rowcount

    async def delete(self, cond, on_conflict=None):
        if self.db_instance is None:
            await self.connect()

        cond_str = ' AND '.join(cond) if isinstance(cond, list) else cond
        query = f"DELETE FROM {self.table} WHERE {cond_str}"

        if on_conflict:
            query += f" ON CONFLICT {on_conflict}"

        async with self.db_instance.execute(query) as cur:
            return cur.rowcount
