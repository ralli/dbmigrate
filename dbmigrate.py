import hashlib
import logging
import re
from base64 import b64encode
from collections import defaultdict, Counter
from datetime import datetime
from logging import info, debug, warning
from pathlib import Path
from typing import Optional, List, Dict
from uuid import uuid4

from sqlalchemy import create_engine, Table, MetaData, Column, String, DateTime, text
from sqlalchemy.engine import Engine

DEPENDS_ON_REGEX = re.compile(r'^--\s+depends:\s+(.*)$')
SOURCES_REGEX = re.compile(r'^--\s+sources:\s+(.*)$')


def create_table_object(schema: Optional[str]) -> Table:
    metadata = MetaData(schema=schema)

    return Table('dbmigrate_log', metadata,
                 Column('id', String(40), nullable=False, primary_key=True),
                 Column('name', String(255), nullable=False),
                 Column('checksum', String(64), nullable=False),
                 Column('created_at', DateTime, nullable=False))


class Migration:
    migration_id: str
    filename: str
    name: str
    checksum: str

    def __init__(self, migration_id: str, filename: str, name: str, checksum: str):
        self.migration_id = migration_id
        self.filename = filename
        self.name = name
        self.checksum = checksum

    def __str__(self):
        return f"migration {self.migration_id} {self.filename} {self.name} {self.checksum}"


class Script:
    migration_id: str
    filename: str
    name: str
    checksum: str
    depends_on: List[str]
    sources: List[str]

    def __init__(self, migration_id: str, filename: str, name: str, checksum: str, depends_on: List[str],
                 sources: List[str]):
        self.migration_id = migration_id
        self.filename = filename
        self.name = name
        self.checksum = checksum
        self.depends_on = depends_on
        self.sources = sources

    def __str__(self):
        return f"script {self.migration_id} {self.filename} {self.name} {self.checksum} {self.depends_on} {self.sources}"


class MigrationBackend:
    def execute_migration(self, migration: Migration):
        pass

    def execute_script(self, migration: Migration):
        pass


def quoted(input: str) -> str:
    result = input.replace("'", "''")
    return f"'{result}'"


class ConsoleMigrationBackend(MigrationBackend):
    def execute_migration(self, migration: Migration):
        with open(migration.filename, "r") as f:
            contents = f.read()
        print(contents)
        statement = f"insert into dbmigrate_log (id, name, checksum, created_at) values ({quoted(migration.migration_id)}, {quoted(migration.name)}, {quoted(migration.checksum)}, {quoted(datetime.now().isoformat())});"
        print(statement)

    def execute_script(self, script: Script):
        with open(script.filename, "r") as f:
            contents = f.read()
        print(contents)
        statement = f"insert into dbmigrate_log (id, name, checksum, created_at) values ({quoted(script.migration_id)}, {quoted(script.name)}, {quoted(script.checksum)}, {quoted(datetime.now().isoformat())});"
        print(statement)


def create_connection() -> Engine:
    url = "sqlite+pysqlite:///dbmigrate.db"
    engine = create_engine(url)
    debug(f"using connection: {url}")
    return engine


def create_migrations_log(engine: Engine, schema: Optional[str] = None):
    table = create_table_object(schema)
    table.metadata.create_all(bind=engine)


def compute_checksum(input: str) -> str:
    checksum = hashlib.sha256(input.encode('utf-8')).digest()
    return b64encode(checksum).decode('utf-8')


def load_newest_checksum_by_name(engine: Engine, migration_name: str) -> Optional[str]:
    with engine.connect() as c:
        result = c.execute(
            text("select checksum from dbmigrate_log where name = :name order by created_at desc"),
            {"name": migration_name}
        )
        row = result.first()
        if row:
            return row.checksum
        else:
            return None


def process_migration(migration_file: Path) -> Migration:
    with open(migration_file, "r") as f:
        contents = f.read()
    checksum = compute_checksum(contents)
    name = migration_file.stem
    migration_id = str(uuid4())
    return Migration(migration_id, str(migration_file), name, checksum)


def process_migrations(migrations_path: Path) -> List[Migration]:
    return [process_migration(migration_file) for migration_file in migrations_path.rglob("*.sql")]


def extract_depends_on(contents: str) -> List[str]:
    lines = contents.splitlines()
    all_dependencies = [DEPENDS_ON_REGEX.match(line).group(1).split(",") for line in lines if
                        DEPENDS_ON_REGEX.match(line)]
    return [dependency.strip() for dependencies in all_dependencies for dependency in dependencies]


def extract_sources(contents: str) -> List[str]:
    lines = contents.splitlines()
    all_dependencies = [SOURCES_REGEX.match(line).group(1).split(",") for line in lines if
                        SOURCES_REGEX.match(line)]
    return [dependency.strip() for dependencies in all_dependencies for dependency in dependencies]


def process_script(script_file: Path) -> Script:
    with open(script_file, "r") as f:
        contents = f.read()
    checksum = compute_checksum(contents)
    name = script_file.stem
    migration_id = str(uuid4())
    depends_on = extract_depends_on(contents)
    sources = extract_sources(contents)
    return Script(migration_id, str(script_file), name, checksum, depends_on, sources)


def process_scripts(scripts_path: Path):
    return [process_script(script_file) for script_file in scripts_path.rglob("*.sql")]


def build_dependency_graph(scripts: List[Script]) -> Dict[str, List[str]]:
    result = defaultdict(lambda: [])
    for script in scripts:
        if script.name not in result:
            result[script.name] = []
        for d in script.depends_on:
            result[d].append(script.name)
    return result


def build_dependency_graph_with_sources(scripts: List[Script]) -> Dict[str, List[str]]:
    result = defaultdict(lambda: [])
    for script in scripts:
        if script.name not in result:
            result[script.name] = []
        for d in script.depends_on:
            result[d].append(script.name)
        for s in script.sources:
            result[s].append(script.name)
    return result


def predecessor_counts(graph: Dict[str, List[str]]) -> Dict[str, int]:
    result = Counter()

    for n in graph.keys():
        result[n] = 0

    for _, edges in graph.items():
        for e in edges:
            result[e] += 1

    return result


def topological_sort(graph: Dict[str, List[str]]) -> List[str]:
    visited = set()
    result = []
    counts = predecessor_counts(graph)
    q = list([n for n, c in counts.items() if c == 0])

    while q:
        current = q.pop()
        visited.add(current)
        result.append(current)
        for e in graph[current]:
            counts[e] -= 1
            if counts[e] == 0:
                q.append(e)
    for c in counts.values():
        if c != 0:
            raise Exception("graph contains cycles")
    return result


def check_migration(engine: Engine, name: str, checksum: str) -> bool:
    db_checksum = load_newest_checksum_by_name(engine, name)
    if db_checksum is None:
        return True
    if db_checksum != checksum:
        warning("migration {} has invalid checksum")
        return False
    return True


def check_script(engine: Engine, name: str, checksum: str) -> bool:
    db_checksum = load_newest_checksum_by_name(engine, name)
    if db_checksum is None:
        return True
    if db_checksum != checksum:
        return True
    return False


def main():
    engine = create_connection()
    create_migrations_log(engine)
    migrations_path = Path("test_scripts", "migrations")
    info(f"migrations_path: {migrations_path}")

    migrations = process_migrations(migrations_path)
    migrations = [m for m in migrations if check_migration(engine, m.name, m.checksum)]

    scripts_path = Path("test_scripts", "scripts")
    info(f"scripts_path: {scripts_path}")
    scripts = process_scripts(scripts_path)
    scripts = [s for s in scripts if check_script(engine, s.name, s.checksum)]
    dependency_graph = build_dependency_graph(scripts)
    backend = ConsoleMigrationBackend()

    for m in migrations:
        backend.execute_migration(m)

    script_map = {}
    for s in scripts:
        script_map[s.name] = s

    sorted_list = topological_sort(dependency_graph)
    for script_name in sorted_list:
        if script_name in script_map:
            s = script_map[script_name]
            backend.execute_script(s)
        else:
            warning(f"{script_name} not in script files")


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)
    main()
