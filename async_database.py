import aiosqlite

class AsyncConnect:
    def __init__(self, path, table):
        self.table = table
        self.path = path
        self.db_instance = aiosqlite.connect(self.path)

    async def close(self):
        if self.db_instance:
            await self.db_instance.close()

    async def select(self, cols: list = [], cond=None, aggr=None, fetchall=False):
        async with self.db_instance:
            cur = await self.db_instance.cursor()
            columns_str = ', '.join(cols) if cols else '*'

            # Convert cond to a string if it's a list
            if cond is not None:
                if isinstance(cond, list):
                    cond = f"WHERE {' and '.join(cond)}"
                elif isinstance(cond, str):
                    cond = f"WHERE {cond}"

            aggregation_str = f"{aggr}" if aggr else columns_str
            cond = cond or ""
            query = f"SELECT {aggregation_str} FROM {self.table} {cond}"

            if fetchall:
                return await (await cur.execute(query)).fetchall()

            return await (await cur.execute(query)).fetchone()

    async def insert(self, values=None, on_conflict=None, return_id=True):
        async with self.db_instance:
            cur = await self.db_instance.cursor()

            if not values:
                query = f"INSERT INTO {self.table} DEFAULT VALUES"
            else:
                columns_str = ', '.join(values.keys())
                values_str = ', '.join(['?' for _ in range(len(values))])
                query = f"INSERT INTO {self.table} ({columns_str}) VALUES ({values_str})"

            if on_conflict:
                query += f" ON CONFLICT {on_conflict}"

            if values:
                await cur.execute(query, list(values.values()))
            else:
                await cur.execute(query)

            await self.db_instance.commit()

            if return_id:
                return cur.lastrowid
            else:
                return None

    async def update(self, values, cond=None, on_conflict=None):
        async with self.db_instance:
            cur = await self.db_instance.cursor()

            set_clause = ', '.join([f"{col} = ?" for col in values.keys()])
            cond_str = ' AND '.join(cond) if cond and isinstance(cond, list) else cond or ''
            query = f"UPDATE {self.table} SET {set_clause} WHERE {cond_str}"

            if on_conflict:
                query += f" ON CONFLICT {on_conflict}"

            await cur.execute(query, list(values.values()))
            await self.db_instance.commit()
            return cur.rowcount

    async def delete(self, cond, on_conflict=None):
        async with self.db_instance:
            cur = await self.db_instance.cursor()
            cond_str = ' AND '.join(cond) if isinstance(cond, list) else cond
            query = f"DELETE FROM {self.table} WHERE {cond_str}"

            if on_conflict:
                query += f" ON CONFLICT {on_conflict}"

            await cur.execute(query)
            await self.db_instance.commit()
            return ().rowcount
