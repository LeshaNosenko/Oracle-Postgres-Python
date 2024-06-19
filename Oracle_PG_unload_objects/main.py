import threading
from tkinter import *
from tkinter import filedialog, messagebox, ttk, Label
import re

import cx_Oracle
import psycopg2
import os
import subprocess
import atexit
from queue import Queue
from datetime import datetime

global scheme
global selected_object

subprocess.run(['runas', '/user:Administrator', 'python', __file__])


def export_functions(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )

    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(
            f"SELECT object_name FROM all_objects WHERE object_type='FUNCTION' AND owner='{selected_scheme}'")
    else:
        cur.execute(
            f"SELECT routine_name FROM information_schema.routines WHERE routine_type='FUNCTION' AND routine_schema='{selected_scheme}'")

    functions = cur.fetchall()
    if not functions:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/functions"):
        os.makedirs(f"{folder_path}/{selected_scheme}/functions")
        try:
            with open(f"{folder_path}/{selected_scheme}/functions.xml", "w", encoding='utf-8') as xml_file:
                xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
                xml_file.write(
                    " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
                for function in functions:
                    xml_file.write(
                        f"\t<changeSet author='{author}' id='create_{function[0]}' context='all' runOnChange='true'>\n")
                    xml_file.write(
                        f"\t\t<sqlFile path='{selected_scheme}/functions/{function[0]}.sql' splitStatements='false'/>\n")
                    xml_file.write("\t</changeSet>\n")
                xml_file.write("</databaseChangeLog>\n")
            for function in functions:
                if select_tab == 'Oracle':
                    cur.execute(
                        f"SELECT dbms_metadata.get_ddl('FUNCTION','{function[0]}','{selected_scheme}') FROM dual")
                    function_def = cur.fetchone()[0]
                    with open(f"{folder_path}/{selected_scheme}/functions/{function[0]}.sql", "w",
                              encoding='utf-8') as func_file:
                        func_file.write(function_def.read())
                else:
                    cur.execute(f"SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = '{function[0]}'")
                    function_def = cur.fetchone()[0]
                    with open(f"{folder_path}/{selected_scheme}/functions/{function[0]}.sql", "w",
                              encoding='utf-8') as func_file:
                        func_file.write(function_def[0])
            queue.put(1)
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            queue.put(0)
            return


def export_constraints(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )

    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(
            f"SELECT constraint_name FROM all_constraints WHERE owner = '{selected_scheme}'")
    else:
        cur.execute(
            f"SELECT pg_class.relname as table_name, pg_get_constraintdef(pg_constraint.oid) as constraint_definition, pg_constraint.conname as constraint_name FROM pg_constraint JOIN pg_class ON pg_constraint.conrelid = pg_class.oid WHERE pg_constraint.contype = 'f' AND pg_class.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{selected_scheme}')")

    constraints = cur.fetchall()

    if not constraints:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/constraints"):
        os.makedirs(f"{folder_path}/{selected_scheme}/constraints")

    if select_tab == 'Oracle':
        try:
            with open(f"{folder_path}/{selected_scheme}/constraints.xml", "w", encoding='utf-8') as xml_file:
                xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
                xml_file.write(
                    "   <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
                for constraint in constraints:
                    try:
                        cur.execute(
                            f"SELECT dbms_metadata.get_dependent_ddl('CONSTRAINT', '{constraint[0]}', '{selected_scheme}') FROM dual")
                        ddl_data = cur.fetchall()

                        with open(f"{folder_path}/{selected_scheme}/constraints/{constraint[0]}.sql", "w",
                                  encoding='utf-8') as sql_file:
                            for ddl_str in ddl_data:
                                if ddl_str[0] is not None:
                                    sql_file.write(ddl_str[0].read() + ';\n')
                                    xml_file.write(
                                        f"<changeSet author='{author}' id='create_{constraint[0]}' context='all' runOnChange='true'>\n")
                                    xml_file.write(
                                        f"<sqlFile path='{selected_scheme}/tables/{constraint[0]}.sql' splitStatements='true' endDelimiter=';'/>\n")
                                    xml_file.write("</changeSet>\n")

                    except cx_Oracle.DatabaseError as e:
                        print(f"Ограничения {constraint[0]} не существует в схеме {selected_scheme}")
            xml_file.write("</databaseChangeLog>\n")
            queue.put(1)
        except:
            queue.put(0)
            return
    else:
        try:
            with open(f"{folder_path}/{selected_scheme}/constraints.xml", "w", encoding='utf-8') as xml_file:
                xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
                xml_file.write(
                    "<databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
                for constraint in constraints:
                    xml_file.write(
                        f"\t<changeSet author='{author}' id='create_{constraint[2]}' context='all' runOnChange='true'>\n")
                    xml_file.write(
                        f"\t\t<sqlFile path='{selected_scheme}/constraints/{constraint[2]}.sql' splitStatements='false'/>\n")
                    xml_file.write("\t</changeSet>\n")
                    with open(f"{folder_path}/{selected_scheme}/constraints/{constraint[2]}.sql",
                              "w", encoding='utf-8') as constraint_file:
                        constraint_file.write(
                            f"do $$\nbegin\nexecute 'ALTER TABLE {selected_scheme}.{constraint[2]} drop constraint if exists {constraint[1]}';\n")
                        constraint_file.write(
                            f"execute 'ALTER TABLE {selected_scheme}.{constraint[2]} ADD CONSTRAINT {constraint[1]} {constraint[0]}';\n")
                        constraint_file.write(
                            "exception when others then null;\n")
                        constraint_file.write(
                            "end $$;")
            with open(f"{folder_path}/{selected_scheme}/constraints.xml", "a", encoding='utf-8') as xml_file:
                xml_file.write("</databaseChangeLog>\n")
            queue.put(1)
        except:
            queue.put(0)
            return


def export_packages(selected_scheme, queue, author):
    dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                service_name=variables_oracle[2].get())
    conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)

    cur = conn.cursor()

    cur.execute(
        f"SELECT object_name FROM all_objects WHERE object_type='PACKAGE' AND owner='{selected_scheme}'")

    packages = cur.fetchall()
    if not packages:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/packages"):
        os.makedirs(f"{folder_path}/{selected_scheme}/packages")
        try:
            with open(f"{folder_path}/{selected_scheme}/packages.xml", "a", encoding='utf-8') as xml_file:
                xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
                xml_file.write(
                    "<databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
                for package in packages:
                    cur.execute(f"SELECT dbms_metadata.get_ddl('PACKAGE','{package[0]}','{selected_scheme}') FROM dual")
                    package_def = cur.fetchall()
                    for pack in package_def:
                        xml_file.write(
                            f"\t<changeSet author='{author}' id='create_{package[0]}' context='all' runOnChange='true'>\n")
                        xml_file.write(
                            f"\t\t<sqlFile path='{selected_scheme}/packages/{package[0]}.sql' splitStatements='false'/>\n")
                        xml_file.write("\t</changeSet>\n")
                        with open(
                                f"{folder_path}/{selected_scheme}/packages/{package[0]}.sql",
                                "w", encoding='utf-8') as package_file:
                            package_file.write(pack[0].read())
                xml_file.write("</databaseChangeLog>\n")
                queue.put(1)
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            queue.put(0)
            return


def export_procedures(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )

    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(
            f"SELECT object_name FROM all_objects WHERE object_type='PROCEDURE' AND owner='{selected_scheme}'")
    else:
        cur.execute(
            f"SELECT routine_name, routine_type, routine_definition FROM information_schema.routines WHERE routine_type='PROCEDURE' AND routine_schema='{selected_scheme}'")

    procedures = cur.fetchall()
    if not procedures:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/procedures"):
        os.makedirs(f"{folder_path}/{selected_scheme}/procedures")

    with open(f"{folder_path}/{selected_scheme}/procedures.xml", "w", encoding='utf-8') as xml_file:
        xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
        for proc in procedures:
            xml_file.write(
                f"\t<changeSet author='{author}' id='create_{proc[0]}' context='all' runOnChange='true'>\n")
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/procedures/{proc[0]}.sql' splitStatements='false'/>\n")
            xml_file.write("\t</changeSet>\n")
        xml_file.write("</databaseChangeLog>\n")
    for proc in procedures:
        if select_tab == 'Oracle':
            cur.execute(
                f"SELECT dbms_metadata.get_ddl('PROCEDURE','{proc[0]}','{selected_scheme}') FROM dual")
            procedure_def = cur.fetchone()[0]
            with open(f"{folder_path}/{selected_scheme}/procedures/{proc[0]}.sql", "w", encoding='utf-8') as proc_file:
                proc_file.write(procedure_def.read())
        else:
            cur.execute(f"SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = '{proc[0]}'")
            procedure_def = cur.fetchone()[0]
            with open(f"{folder_path}/{selected_scheme}/procedures/{proc[0]}.sql", "w", encoding='utf-8') as proc_file:
                proc_file.write(procedure_def[0])
    queue.put(1)


def export_synonyms(selected_scheme, queue, author):
    dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                service_name=variables_oracle[2].get())
    conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)

    cur = conn.cursor()

    cur.execute(
        f"SELECT object_name FROM all_objects WHERE object_type='SYNONYM' AND owner='{selected_scheme}'")

    synonyms = cur.fetchall()
    if not synonyms:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/synonyms"):
        os.makedirs(f"{folder_path}/{selected_scheme}/synonyms")
        try:
            with open(f"{folder_path}/{selected_scheme}/packages.xml", "a", encoding='utf-8') as xml_file:
                xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
                xml_file.write(
                    "<databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
                for synonym in synonyms:
                    cur.execute(f"SELECT dbms_metadata.get_ddl('SYNONYM','{synonym[0]}','{selected_scheme}') FROM dual")
                    synonym_def = cur.fetchall()
                    for syn in synonym_def:
                        xml_file.write(
                            f"\t<changeSet author='{author}' id='create_{synonym[0]}' context='all' runOnChange='true'>\n")
                        xml_file.write(
                            f"\t\t<sqlFile path='{selected_scheme}/synonyms/{synonym[0]}.sql' splitStatements='false'/>\n")
                        xml_file.write("\t</changeSet>\n")
                        with open(
                                f"{folder_path}/{selected_scheme}/synonyms/{synonym[0]}.sql",
                                "w", encoding='utf-8') as synonym_file:
                            synonym_file.write(syn[0].read())
                    xml_file.write("</databaseChangeLog>\n")
                queue.put(1)
        except Exception as e:
            print(f"Произошла ошибка: {e}")
            queue.put(0)
            return


def export_tables(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )
    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(
            f"SELECT object_name FROM all_objects WHERE object_type='TABLE' AND owner='{selected_scheme}'")
    else:
        cur.execute(
            f"SELECT table_name FROM information_schema.tables WHERE table_schema='{selected_scheme}'")

    tables = cur.fetchall()
    if not tables:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/tables"):
        os.makedirs(f"{folder_path}/{selected_scheme}/tables")

    if select_tab == 'Oracle':
        with open(f"{folder_path}/{selected_scheme}/tables.xml", "w", encoding='utf-8') as xml_file:
            xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
            xml_file.write(
                "   <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
            for table in tables:
                xml_file.write(
                    f"<changeSet author='{author}' id='create_{table[0]}' context='all' runOnChange='true'>\n")
                xml_file.write(
                    f"<sqlFile path='{selected_scheme}/tables/{table[0]}.sql' splitStatements='true' endDelimiter=';'/>\n")
                xml_file.write("</changeSet>\n")
                try:
                    cur.execute(f"SELECT dbms_metadata.get_ddl('TABLE', '{table[0]}', '{selected_scheme}') FROM dual")
                    ddl_data = cur.fetchall()

                    try:
                        cur.execute(
                            f"SELECT dbms_metadata.get_dependent_ddl('COMMENT', '{selected_scheme}', '{table[0]}') FROM dual")
                        ddl_comment = cur.fetchall()
                    except cx_Oracle.DatabaseError:
                        print(f"Комментарии в схеме {selected_scheme} таблицы {table[0]} не найден")
                        ddl_comment = None

                    try:
                        cur.execute(f"SELECT listagg(dbms_metadata.get_ddl('INDEX', index_name, '{selected_scheme}'), '') WITHIN GROUP (ORDER BY 1) \
                                        FROM dba_indexes \
                                        WHERE table_name = '{table[0]}' AND owner = '{selected_scheme}'")
                        ddl_index = cur.fetchall()
                    except cx_Oracle.DatabaseError:
                        print(f"Индексы в схеме {selected_scheme} таблицы {table[0]} не найден.")
                        ddl_index = None

                    with open(f"{folder_path}/{selected_scheme}/tables/{table[0]}.sql", "w",
                              encoding='utf-8') as sql_file:
                        for ddl_str in ddl_data:
                            if ddl_str[0] is not None:
                                sql_file.write(ddl_str[0].read() + ';\n')

                        if ddl_comment:
                            for ddl_str in ddl_comment:
                                if ddl_str[0] is not None:
                                    sql_file.write(ddl_str[0] + ';\n')

                        if ddl_index:
                            for ddl_str in ddl_index:
                                if ddl_str[0] is not None:
                                    sql_file.write(ddl_str[0] + ';\n')
                except cx_Oracle.DatabaseError as e:
                    error, = e.args
                    if error.code == 31603:
                        print(f"Таблицы {table[0]} не существует в схеме {selected_scheme}")
                    else:
                        raise
            queue.put(1)
    else:
        with open(f"{folder_path}/{selected_scheme}/tables.xml", "w", encoding='utf-8') as xml_file:
            xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
            xml_file.write(
                "   <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
            for table in tables:
                xml_file.write(
                    f"<changeSet author='{author}' id='create_{table[0]}' context='all' runOnChange='true'>\n")
                xml_file.write(
                    f"<sqlFile path='{selected_scheme}/tables/{table[0]}.sql' splitStatements='true' endDelimiter=';'/>\n")
                xml_file.write("</changeSet>\n")
                cur.execute(
                    f"SELECT column_name, udt_name, is_nullable, case when column_default like '%_seq%' then null else column_default end, ordinal_position::int FROM information_schema.columns WHERE table_name='{table[0]}' and table_schema = '{selected_scheme}'")
                columns = cur.fetchall()
                if columns and columns[0] is not None:
                    with open(f"{folder_path}/{selected_scheme}/tables/{table[0]}.sql", "a",
                              encoding='utf-8') as sql_file:
                        sql_file.write(f"CREATE TABLE IF NOT EXISTS {selected_scheme}.{table[0]} (\n")
                        for i, column in enumerate(columns):
                            sql_file.write(f"\"{column[0]}\" {column[1]}")
                            if column[2] == 'NO':
                                sql_file.write(" NOT NULL")
                                if column[3] is not None and column[0] != 'id':
                                    sql_file.write(f" DEFAULT {column[3]}")
                            if i != len(columns) - 1:
                                sql_file.write(",")
                            sql_file.write("\n")
                        if i != len(columns) - 1:
                            sql_file.write(",")
                        cur.execute(
                            f"SELECT pg_get_constraintdef(pg_constraint.oid) as constraint_definition, pg_constraint.conname as constraint_name FROM pg_constraint JOIN pg_class ON pg_constraint.conrelid = pg_class.oid WHERE pg_constraint.contype = 'p' AND pg_class.relname = '{table[0]}' limit 1")
                        pk_keys = cur.fetchall()
                        if pk_keys is not None:
                            for pk_key in pk_keys:
                                sql_file.write(f",CONSTRAINT {pk_key[1]} {pk_key[0]}")
                        sql_file.write(");\n")
                        sql_file.write("\n")

                cur.execute(
                    f"SELECT obj_description((c.table_schema || '.' || c.table_name)::regclass) FROM information_schema.tables c WHERE c.table_name ='{table[0]}'")
                table_comment = cur.fetchone()
                if table_comment is not None and table_comment[0] is not None:
                    comment = table_comment[0].replace("(", '').replace(",)", '')
                    with open(f"{folder_path}/{selected_scheme}/tables/{table[0]}.sql", "a",
                              encoding='utf-8') as sql_file:
                        sql_file.write(
                            f"COMMENT ON TABLE {selected_scheme}.{table[0]} IS '{comment}';\n\n")
                    for column in columns:
                        cur.execute(
                            f"SELECT col_description(oid,'{column[4]}') FROM pg_class WHERE relname='{table[0]}'")
                        column_comment = cur.fetchone()
                        if column_comment and column_comment[0] is not None:
                            comment = column_comment[0].replace("(", '').replace(",)", '')
                            with open(f"{folder_path}/{selected_scheme}/tables/{table[0]}.sql",
                                      "a", encoding='utf-8') as sql_file:
                                sql_file.write(
                                    f"COMMENT ON COLUMN {selected_scheme}.{table[0]}.\"{column[0]}\" IS '{comment}';\n")
                    cur.execute(
                        f"Select i.relname, pg_get_indexdef(ix.indexrelid) from pg_class t, pg_class i, pg_index ix, pg_attribute a where t.oid = ix.indrelid  and i.oid = ix.indexrelid  and a.attrelid = t.oid  and a.attnum = ANY(ix.indkey)  and t.relkind = 'r'  and t.relname = '{table[0]}' AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{selected_scheme}')")
                    indexes = cur.fetchall()
                    with open(f"{folder_path}/{selected_scheme}/tables/{table[0]}.sql", "a",
                              encoding='utf-8') as sql_file:
                        sql_file.write("\n")
                        for index in indexes:
                            index_def = index[1]
                            if "UNIQUE" in index_def:
                                index_def = index_def.replace("UNIQUE INDEX",
                                                              "UNIQUE INDEX IF NOT EXISTS")
                            elif "CREATE INDEX" in index_def:
                                index_def = index_def.replace("CREATE INDEX",
                                                              "CREATE INDEX IF NOT EXISTS")
                                sql_file.write(index_def)
                                sql_file.write(";")
                                sql_file.write('\n')
    with open(f"{folder_path}/{selected_scheme}/tables.xml", "a", encoding='utf-8') as xml_file:
        xml_file.write("</databaseChangeLog>\n")
    queue.put(1)


def export_roles(selected_scheme, queue, author):
    conn = psycopg2.connect(
        host=variables_pg[0].get(),
        port=variables_pg[1].get(),
        database=variables_pg[2].get(),
        user=variables_pg[3].get(),
        password=variables_pg[4].get()
    )
    cur = conn.cursor()

    cur.execute(
        "SELECT 'CREATE ROLE \"'||r.rolname||'\" WITH  PASSWORD '''||r.rolname||''' '||case when rolcanlogin is true then 'LOGIN' else 'NOLOGIN' end ||' '||case when rolsuper is true then 'SUPERUSER' else 'NOSUPERUSER' end ||' '||case when rolinherit is true then 'INHERIT' else 'NOINHERIT' end ||'  '||case when rolcreatedb is true then 'CREATEDB' else 'NOCREATEDB' end ||' '||case when rolcreaterole is true then 'CREATEROLE' else 'NOCREATEROLE' end ||' '||case when rolreplication is true then 'REPLICATION' else 'NOREPLICATION' end ||' VALID UNTIL ''infinity''; ' as a, r.rolname from ( select r.* , ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) as memberof FROM pg_roles r where r.rolname not like 'pg_%' order by r.rolname) r")
    roles = cur.fetchall()

    cur.execute(f"SELECT 'CREATE EXTENSION IF NOT EXISTS ' as a, extname as b FROM pg_extension")
    extensions = cur.fetchall()

    if not roles:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/roles"):
        os.makedirs(f"{folder_path}/{selected_scheme}/roles")

    with open(f"{folder_path}/{selected_scheme}/roles.xml", "w", encoding='utf-8') as xml_file:
        xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
        xml_file.write(
            f"\t<changeSet author='{author}' id='create_roles' context='all' runOnChange='true'>\n")
        xml_file.write(
            f"\t\t<sqlFile path='{selected_scheme}/roles/roles.sql' splitStatements='false'/>\n")
        xml_file.write("\t</changeSet>\n")
        with open(f"{folder_path}/{selected_scheme}/roles/roles.sql", "a", encoding='utf-8') as role_file:
            for extension in extensions:
                role_file.write(
                    f"do\n$$\nbegin {extension[0]} \"{extension[1]}\";\n end;\n$$;\n\n")
        with open(f"{folder_path}/{selected_scheme}/roles/roles.sql", "a", encoding='utf-8') as role_file:
            for role in roles:
                role_file.write(
                    f"do\n$$\nbegin\nif not exists (select * from pg_roles where rolname = '{role[1]}') then {role[0]}\n end if;\nend;\n$$;\n\n")
        xml_file.write("</databaseChangeLog>\n")
    queue.put(1)


def export_views(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )
    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(f"SELECT view_name FROM all_views WHERE owner='{selected_scheme}'")
    else:
        cur.execute(f"SELECT table_name FROM information_schema.views WHERE table_schema='{selected_scheme}'")

    views = cur.fetchall()

    if not views:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/views"):
        os.makedirs(f"{folder_path}/{selected_scheme}/views")

    with open(f"{folder_path}/{selected_scheme}/views.xml", "w", encoding='utf-8') as xml_file:
        xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
        for view in views:
            view_name = view[0]
            if select_tab == 'Oracle':
                cur.execute(
                    f"SELECT COUNT(*) FROM all_views WHERE owner='{selected_scheme}' AND view_name='{view_name}'")
            else:
                cur.execute(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='{selected_scheme}' AND table_name='{view_name}');")
            if not cur.fetchone()[0]:
                continue

            xml_file.write(
                f"\t<changeSet author='{author}' id='create_{view_name}' context='all' runOnChange='true'>\n")
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/views/{view_name}.sql' splitStatements='false'/>\n")
            xml_file.write("\t</changeSet>\n")

            if select_tab == 'Oracle':
                try:
                    cur.execute(f"SELECT dbms_metadata.get_ddl('VIEW','{view_name}','{selected_scheme}') FROM dual")
                except cx_Oracle.DatabaseError:
                    print(f"DDL Вьюхи {view_name} не существует в схеме {selected_scheme}")
            else:
                cur.execute(f"SELECT pg_get_viewdef('{selected_scheme}.{view_name}', true)")

            view_query = cur.fetchone()

            with open(f"{folder_path}/{selected_scheme}/views/{view_name}.sql", "w", encoding='utf-8') as sql_file:
                if select_tab == 'Oracle':
                    for ddl_str in view_query:
                        if ddl_str is not None:
                            sql_file.write(ddl_str.read() + ';\n')
                else:
                    view_query = view_query[0].replace("'", "")
                    sql_file.write(
                        f"CREATE OR REPLACE VIEW IF NOT EXISTS {selected_scheme}.{view_name} AS \n {view_query}")
        xml_file.write("</databaseChangeLog>\n")
    queue.put(1)


def export_datatypes(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )
    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(
            f"SELECT object_name FROM all_objects WHERE object_type='TYPE' AND owner='{selected_scheme}'")
    else:
        cur.execute("""
        		SELECT n.nspname, typname,
        			   pg_catalog.array_to_string(array_agg('''' || e.enumlabel || '''' ORDER BY enumsortorder), ',') AS enum_values,
        			   (SELECT pg_catalog.array_to_string(array_agg(a.attname || ' ' || pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum), ',') custom_type_values
        				FROM pg_catalog.pg_attribute a
        				INNER JOIN pg_catalog.pg_class c ON (a.attrelid=c.oid)
        				WHERE a.attnum > 0 AND NOT a.attisdropped AND c.oid=t.typrelid)
        		FROM pg_catalog.pg_type t
        		LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
        		LEFT JOIN pg_catalog.pg_enum e ON t.oid = e.enumtypid
        		WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
        			AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
        			AND pg_catalog.pg_type_is_visible(t.oid) and n.nspname != 'pg_catalog'
        		GROUP BY 1, 2, t.typrelid;
        	""")

    datatypes = cur.fetchall()

    if not datatypes:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/datatypes"):
        os.makedirs(f"{folder_path}/{selected_scheme}/datatypes")

    if select_tab == 'Oracle':
        ddl_data = []
        for data_type in datatypes:
            try:
                cur.execute(
                    f"SELECT dbms_metadata.get_ddl('TYPE', '{data_type[0]}', '{selected_scheme}')  || ';' FROM dual")
                ddl_data = cur.fetchall()
            except cx_Oracle.DatabaseError:
                print(f"В схеме {selected_scheme} тип {data_type[0]} не найден")

            with open(f"{folder_path}/{selected_scheme}/datatypes/{data_type[0]}.sql", "w",
                      encoding='utf-8') as sql_file:

                for ddl_str in ddl_data:
                    if ddl_str[0] is not None:
                        cleaned_str = re.sub('\s+', ' ', ddl_str[0].read())
                        cleaned_str = re.sub('( )( )+', ' ', cleaned_str)
                        sql_file.write(cleaned_str + '\n/')

        queue.put(1)
    else:
        for datatype in datatypes:
            with open(f"{folder_path}/{selected_scheme}/datatypes/{datatype[1]}.sql", "w",
                      encoding='utf-8') as sql_file:
                if datatype[2]:
                    values = f"AS ENUM ({datatype[2]})"
                elif datatype[3]:
                    values = f"AS ({datatype[3]})"
                else:
                    values = ""
                sql_file.write(
                    f"do\n$$\nbegin\nif not exists (select * from pg_type where typname = '{datatype[1]}') then CREATE TYPE {datatype[1]} {values};\n end if;\nend;\n$$;\n\n")
            sql_file.close()

        with open(f"{folder_path}/{selected_scheme}/datatypes.xml", "w", encoding='utf-8') as xml_file:
            xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
            xml_file.write(
                " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
            for datatype in datatypes:
                xml_file.write(
                    f"\t<changeSet author='{author}' id='create_{datatype[1]}' context='all' runOnChange='true'>\n")
                xml_file.write(
                    f"\t\t<sqlFile path='{selected_scheme}/datatypes/{datatype[1]}.sql' splitStatements='false'/>\n")
                xml_file.write("\t</changeSet>\n")
            xml_file.write("</databaseChangeLog>\n")
        queue.put(1)


def export_matviews(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )
    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(f"SELECT mview_name FROM all_mviews  WHERE owner='{selected_scheme}'")
    else:
        cur.execute(f"SELECT matviewname FROM pg_matviews WHERE schemaname='{selected_scheme}'")

    matviews = cur.fetchall()
    if not matviews:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/matviews"):
        os.makedirs(f"{folder_path}/{selected_scheme}/matviews")

    with open(f"{folder_path}/{selected_scheme}/matviews.xml", "w", encoding='utf-8') as xml_file:
        xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
        for matview in matviews:
            matview_name = matview[0]
            if select_tab == 'Oracle':
                cur.execute(
                    f"SELECT COUNT(*) FROM all_views WHERE owner='{selected_scheme}' AND view_name='{matview_name}'")
            else:
                cur.execute(
                    f"SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='{selected_scheme}' AND table_name='{matview_name}');")
            if not cur.fetchone()[0]:
                continue

            xml_file.write(
                f"\t<changeSet author='{author}' id='create_{matview_name}' context='all' runOnChange='true'>\n")
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/matviews/{matview_name}.sql' splitStatements='false'/>\n")
            xml_file.write("\t</changeSet>\n")

            if select_tab == 'Oracle':
                try:
                    cur.execute(
                        f"SELECT dbms_metadata.get_ddl('MATERIALIZED_VIEW','{matview_name}','{selected_scheme}') FROM dual")
                except cx_Oracle.DatabaseError:
                    print(f"DDL Вьюхи {matview_name} не существует в схеме {selected_scheme}")
            else:
                cur.execute(f"SELECT pg_get_viewdef('{selected_scheme}.{matview_name}', true)")

            view_query = cur.fetchone()

            with open(f"{folder_path}/{selected_scheme}/matviews/{matview_name}.sql", "w",
                      encoding='utf-8') as sql_file:
                if select_tab == 'Oracle':
                    for ddl_str in view_query:
                        if ddl_str is not None:
                            sql_file.write(ddl_str.read() + ';\n')
                else:
                    view_query = view_query[0].replace("'", "")
                    sql_file.write(
                        f"CREATE MATERIALIZED VIEW IF NOT EXISTS {selected_scheme}.{matview_name} AS \n {view_query}")
        xml_file.write("</databaseChangeLog>\n")
    queue.put(1)


def export_triggers(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )

    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(f"SELECT trigger_name FROM all_triggers WHERE owner = '{selected_scheme}'")
    else:
        cur.execute(
            f"SELECT trigger_name, 'CREATE TRIGGER ' || trigger_name || ' ' || action_timing || ' ' || string_agg(event_manipulation, ' OR ') || ' ON ' || event_object_table || ' FOR EACH ROW ' || action_statement || ';' as ddl, event_object_table FROM information_schema.triggers WHERE event_object_schema='{selected_scheme}' GROUP BY trigger_name, action_timing, event_object_table, action_statement;")
    triggers = cur.fetchall()

    if not triggers:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/triggers"):
        os.makedirs(f"{folder_path}/{selected_scheme}/triggers")

    with open(f"{folder_path}/{selected_scheme}/triggers.xml", "w", encoding='utf-8') as xml_file:
        xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
        for trigger in triggers:
            xml_file.write(
                f"\t<changeSet author='{author}' id='create_{trigger[0]}' context='all' runOnChange='true'>\n")
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/triggers/{trigger[0]}.sql' splitStatements='false'/>\n")
            xml_file.write("\t</changeSet>\n")
        xml_file.write("</databaseChangeLog>\n")

    for trigger in triggers:
        with open(f"{folder_path}/{selected_scheme}/triggers/{trigger[0]}.sql", "w", encoding='utf-8') as sql_file:
            if select_tab == 'Oracle':
                try:
                    cur.execute(f"SELECT dbms_metadata.get_ddl('TRIGGER','{trigger[0]}','{selected_scheme}') FROM dual")
                except cx_Oracle.DatabaseError as e:
                    print(f"DDL Триггера {trigger[0]} не существует в схеме {selected_scheme}")


            if not cur.fetchone()[0]:
                trigger_query = cur.fetchone()[0]
                continue

            with open(f"{folder_path}/{selected_scheme}/triggers/{trigger[0]}.sql", "w", encoding='utf-8') as sql_file:
                if select_tab == 'Oracle':
                    for ddl_str in trigger_query:
                        if ddl_str is not None:
                            sql_file.write(ddl_str.read() + ';\n')
                else:
                    sql_file.write(
                        f"DO $$ BEGIN IF EXISTS (SELECT * FROM information_schema.triggers WHERE event_object_schema = '{selected_scheme}' AND trigger_name = '{trigger[0]}') THEN EXECUTE 'DROP TRIGGER IF EXISTS {trigger[0]} ON {trigger[2]}'; EXECUTE '{trigger[1]}\n''; END IF; END $$;\n\n")
        sql_file.close()
    queue.put(1)


def export_grants(selected_scheme, queue, select_tab, author):
    if select_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
    else:
        conn = psycopg2.connect(
            host=variables_pg[0].get(),
            port=variables_pg[1].get(),
            database=variables_pg[2].get(),
            user=variables_pg[3].get(),
            password=variables_pg[4].get()
        )
    cur = conn.cursor()

    if select_tab == 'Oracle':
        cur.execute(f"SELECT grantor FROM all_tab_privs_recd WHERE owner = '{selected_scheme}'")
    else:
        cur.execute(f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{selected_scheme}'")

    grants = cur.fetchall()

    if not grants:
        queue.put(0)
        return

    if not os.path.exists(f"{folder_path}/{selected_scheme}/grants"):
        os.makedirs(f"{folder_path}/{selected_scheme}/grants")

        with open(f"{folder_path}/{selected_scheme}/grants.xml", "w", encoding='utf-8') as xml_file:
            xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
            xml_file.write(
                " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n")
            xml_file.write(
                f"\t<changeSet author='{author}' id='grants' context='all' runOnChange='true'>\n")
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/grants/grants.sql' splitStatements='false'/>\n")
            xml_file.write("\t</changeSet>\n")
            xml_file.write("</databaseChangeLog>\n")

            if select_tab == 'Oracle':

                system_grant_query = None
                role_grant_query = None
                objects_grant_query = None

                try:
                    cur.execute(f"SELECT dbms_metadata.get_granted_ddl('SYSTEM_GRANT','{selected_scheme}') FROM dual")
                    system_grant_query = cur.fetchone()

                except cx_Oracle.DatabaseError:
                    print(f"DDL SYSTEM_GRANT не существует в схеме {selected_scheme}")

                try:
                    cur.execute(f"SELECT dbms_metadata.get_granted_ddl('ROLE_GRANT','{selected_scheme}') FROM dual")
                    role_grant_query = cur.fetchone()
                except cx_Oracle.DatabaseError:
                    print(f"DDL ROLE_GRANT не существует в схеме {selected_scheme}")

                try:
                    cur.execute("""
                                        SELECT listagg(case
                                                        when     privilege in ('EXECUTE', 'READ', 'WRITE') and table_name not in ('OS_HELPER','FILE_TYPE_JAVA') then
                                                                'GRANT ' || privilege || ' ON DIRECTORY ' || '"' || table_name || '"' || ' TO ' || '"' || grantee || '"' || decode(grantable, 'YES', ' WITH ADMIN OPTION') || ';'
                                                        when     privilege in ('EXECUTE', 'READ', 'WRITE') and table_name in ('OS_HELPER', 'FILE_TYPE_JAVA') then
                                                                    'GRANT ' || privilege || ' ON JAVA SOURCE ' || '"' || owner || '"' || ' TO ' || '"' || grantee || '"' || decode(grantable, 'YES', ' WITH ADMIN OPTION') || ';'
                                                        else
                                                                    'GRANT ' || privilege || ' ON ' || '"' || owner || '"."' || '"' || table_name || '"' || ' TO ' || '"' || grantee || '"' || decode(grantable,'YES', ' WITH ADMIN OPTION') || ';'
                                                   end, chr(13)) WITHIN GROUP (ORDER BY table_name) AS "Object_Grants"
                                        FROM all_tab_privs_recd WHERE grantee = '{selected_scheme}'""")
                    objects_grant_query = cur.fetchall()
                except cx_Oracle.DatabaseError:
                    print(f"DDL Грантов не существует в схеме {selected_scheme}")

                with open(f"{folder_path}/{selected_scheme}/grants/grants.sql", "w", encoding='utf-8') as sql_file:

                    if system_grant_query is not None:
                        for ddl_str in system_grant_query:
                            if ddl_str is not None:
                                sql_file.write(ddl_str.read() + '\n')

                    if role_grant_query is not None:
                        for ddl_str1 in role_grant_query:
                            if ddl_str1 is not None:
                                sql_file.write(ddl_str1.read() + '\n')

                    if objects_grant_query is not None:
                        for ddl_str2 in objects_grant_query:
                            if ddl_str2[0] is not None:
                                sql_file.write(ddl_str2[0] + '\n')
                queue.put(1)

            else:
                with open(f"{folder_path}/{selected_scheme}/grants/grants.sql", "w", encoding='utf-8') as grants_file:
                    cur.execute(
                        f"SELECT distinct c.grantee, c.privilege_type, t.sequence_name FROM information_schema.usage_privileges c join information_schema.sequences t on t.sequence_name = c.object_name and c.object_type = 'SEQUENCE' and c.object_schema='{selected_scheme}'")
                    grants = cur.fetchall()
                    for grant in grants:
                        grantee = grant[0]
                        privilege = grant[1]
                        object_name = grant[2]
                        grants_file.write(
                            f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n")
                    cur.execute(
                        f"SELECT distinct c.grantee, c.privilege_type, c.routine_name FROM information_schema.routine_privileges c where exists(select 1 from information_schema.routines t where t.routine_name = c.routine_name and t.routine_schema='{selected_scheme}' and t.routine_type = 'FUNCTION')")
                    grants = cur.fetchall()
                    for grant in grants:
                        grantee = grant[0]
                        privilege = grant[1]
                        object_name = grant[2]
                        grants_file.write(
                            f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n")
                    cur.execute(
                        f"SELECT distinct c.grantee, c.privilege_type, c.routine_name FROM information_schema.routine_privileges c where exists(select 1 from information_schema.triggers t where t.trigger_name = c.routine_name) and c.routine_schema='{selected_scheme}'")
                    grants = cur.fetchall()
                    for grant in grants:
                        grantee = grant[0]
                        privilege = grant[1]
                        object_name = grant[2]
                        grants_file.write(
                            f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n")
                    cur.execute(
                        f"SELECT distinct c.grantee, c.privilege_type, c.routine_name FROM information_schema.routine_privileges c where exists(select 1 from information_schema.routines t where t.routine_name = c.routine_name and t.routine_schema='{selected_scheme}' and t.routine_type = 'PROCEDURE')")
                    grants = cur.fetchall()
                    for grant in grants:
                        grantee = grant[0]
                        privilege = grant[1]
                        object_name = grant[2]
                        grants_file.write(
                            f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n")
                    cur.execute(
                        f"SELECT distinct c.grantee, c.privilege_type, c.table_name  FROM information_schema.table_privileges c where exists(select 1 from information_schema.views t where t.table_name = c.table_name) and c.table_schema='{selected_scheme}'")
                    grants = cur.fetchall()
                    for grant in grants:
                        grantee = grant[0]
                        privilege = grant[1]
                        object_name = grant[2]
                        grants_file.write(
                            f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n")
                    cur.execute(
                        f"SELECT distinct coalesce(nullif(s[1], ''), '{selected_scheme}') grantee, CASE ch WHEN 'r' THEN 'SELECT' WHEN 'w' THEN 'UPDATE' WHEN 'a' THEN 'INSERT' WHEN 'd' THEN 'DELETE' WHEN 'D' THEN 'TRUNCATE' WHEN 'x' THEN 'REFERENCES' WHEN 't' THEN 'TRIGGER' END AS privilege,relname FROM  pg_class  JOIN pg_namespace ON pg_namespace.oid = relnamespace JOIN pg_roles ON pg_roles.oid = relowner, unnest(coalesce(relacl::text[], format('{{%s=arwdDxt/%s}}', rolname, rolname)::text[])) AS acl, regexp_split_to_array(acl, '=|/') AS s, regexp_split_to_table(s[2], '') ch WHERE nspname ='{selected_scheme}'")
                    grants = cur.fetchall()
                    for grant in grants:
                        grantee = grant[0]
                        privilege = grant[1]
                        object_name = grant[2]
                        grants_file.write(
                            f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n")
                queue.put(1)


def clear_log():
    log_listbox_pg.delete(0, END)
    log_listbox_oracle.delete(0, END)


def load_schema_for_exclude():
    current_index = tab_control.index('current')

    selected_tab = tab_control.tab(current_index, "text")

    if selected_tab == 'Oracle':
        dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                    service_name=variables_oracle[2].get())
        try:
            conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)
            cur = conn.cursor()
            cur.execute(
                "SELECT username FROM all_users WHERE username NOT IN ('SYS', 'SYSTEM','TOAD')"
            )
            all_schemes = cur.fetchall()
            tree_scheme_oracle.delete(0, END)
            for schema_name in all_schemes:
                tree_scheme_oracle.insert(END, schema_name)
        except cx_Oracle.OperationalError as e:
            messagebox.showerror("Не возможно подключиться к серверу бд", str(e))
            return
        except cx_Oracle.DatabaseError as e:
            messagebox.showerror("Ошибка бд", str(e))
            return

    else:
        try:
            conn = psycopg2.connect(
                host=variables_pg[0].get(),
                port=variables_pg[1].get(),
                database=variables_pg[2].get(),
                user=variables_pg[3].get(),
                password=variables_pg[4].get()
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name NOT LIKE 'information_schema'")
            all_schemes = cur.fetchall()
            tree_scheme_pg.delete(0, END)
            for schema_name in all_schemes:
                tree_scheme_pg.insert(END, schema_name)

        except psycopg2.OperationalError as e:
            messagebox.showerror("Не возможно подключиться к серверу бд", str(e))
            return
        except psycopg2.DatabaseError as e:
            messagebox.showerror("Ошибка бд", str(e))
            return


def export_selected_objects():
    import os
    import shutil
    from threading import Thread
    global folder_path

    current_index = tab_control.index('current')

    selected_tab = tab_control.tab(current_index, "text")

    if selected_tab == 'Oracle':
        if not any(var.get() for var in vars_oracle):
            messagebox.showerror("Ошибка", "Выберите хотя-бы один тип объекта для экспорта")
            return
    else:
        if not any(var.get() for var in vars_pg):
            messagebox.showerror("Ошибка", "Выберите хотя-бы один тип объекта для экспорта")
            return

    queue1 = Queue()
    queue2 = Queue()
    queue3 = Queue()
    queue4 = Queue()
    queue5 = Queue()
    queue6 = Queue()
    queue7 = Queue()
    queue8 = Queue()
    queue9 = Queue()
    queue10 = Queue()
    queue11 = Queue()
    folder_path = filedialog.askdirectory()

    if selected_tab == 'Oracle':
        try:
            dsn_tns = cx_Oracle.makedsn(variables_oracle[0].get(), variables_oracle[1].get(),
                                        service_name=variables_oracle[2].get())
            conn = cx_Oracle.connect(user=variables_oracle[3].get(), password=variables_oracle[4].get(), dsn=dsn_tns)

            cur = conn.cursor()

            selected_indices = tree_scheme_oracle.curselection()
            selected_schemas = [tree_scheme_oracle.get(i)[0] for i in selected_indices]

            if selected_schemas:
                sql = "SELECT username FROM all_users"
                selected_schemas += ['SYS', 'SYSTEM', 'TOAD']
                sql += " WHERE username NOT IN ("
                sql += ', '.join([f':{i + 1}' for i in range(len(selected_schemas))]) + ")"
                params = {f'{i + 1}': schema for i, schema in enumerate(selected_schemas)}
                cur.execute(sql, params if params else None)
            else:
                sql = "SELECT username FROM all_users where not in ('SYS', 'SYSTEM', 'TOAD')"
                cur.execute(sql)

            all_schemes = cur.fetchall()

            author = variables_oracle[5].get()

        except cx_Oracle.OperationalError as e:
            messagebox.showerror("Не возможно подключиться к серверу бд", str(e))
            return
    else:
        try:
            conn = psycopg2.connect(
                host=variables_pg[0].get(),
                port=variables_pg[1].get(),
                database=variables_pg[2].get(),
                user=variables_pg[3].get(),
                password=variables_pg[4].get()
            )
            cur = conn.cursor()

            selected_indices = tree_scheme_pg.curselection()
            selected_schemas = [tree_scheme_pg.get(i) for i in selected_indices]

            sql = (
                "SELECT schema_name FROM information_schema.schemata "
                "WHERE schema_name NOT LIKE 'pg_%%' "
                "AND schema_name NOT LIKE 'information_schema' "
            )

            if selected_schemas:
                placeholders = ', '.join(['%s'] * len(selected_schemas))
                sql += "AND schema_name NOT IN (" + placeholders + ")"

            cur.execute(sql, tuple(selected_schemas) if selected_schemas else ())

            all_schemes = cur.fetchall()
            author = variables_pg[5].get()

        except psycopg2.OperationalError as e:
            messagebox.showerror("Не возможно подключиться к серверу бд", str(e))
            return

    messagebox.showinfo("Экспорт", "Экспорт объектов запущен!")

    clear_log()

    if os.path.exists(f"{folder_path}/install.xml"):
        os.remove(f"{folder_path}/install.xml")

    with open(f"{folder_path}/install.xml", "a", encoding='utf-8') as xml_file:
        xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog'\n xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\nxsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog\nhttp://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n\n")

    current_time_start_export = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if selected_tab == 'Oracle':
        log_listbox_oracle.insert(END, f"{current_time_start_export}: Запущен экспорт")
    else:
        log_listbox_pg.insert(END, f"{current_time_start_export}: Запущен экспорт")

    for schema_name in all_schemes:

        with open(f"{folder_path}/install.xml", "a", encoding='utf-8') as xml_file:
            xml_file.write(f"\t<include file='{schema_name[0]}/{schema_name[0]}.xml'/>\n")
            xml_file.close()

        current_time_export_objects = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        if selected_tab == 'Oracle':
            log_listbox_oracle.insert(END,
                                      f"{current_time_export_objects}: Выгружается схема {schema_name[0]} в директорию {folder_path}")
        else:
            log_listbox_pg.insert(END,
                                  f"{current_time_export_objects}: Выгружается схема {schema_name[0]} в директорию {folder_path}")

        if os.path.exists(f"{folder_path}/{schema_name[0]}"):
            shutil.rmtree(os.path.join(folder_path, schema_name[0]))

        if not os.path.exists(f"{folder_path}/{schema_name[0]}"):
            os.makedirs(f"{folder_path}/{schema_name[0]}")

        if os.path.exists(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml"):
            os.remove(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml")

        with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "w", encoding='utf-8') as xml_file:
            xml_file.write("<?xml version='1.0' encoding='UTF-8'?>\n")
            xml_file.write(
                " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog'\n xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\nxsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog\nhttp://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n\n")
            xml_file.close()

            if selected_tab == 'Oracle':

                grants_var = vars_oracle[0]
                datatypes_var = vars_oracle[1]
                functions_var = vars_oracle[2]
                procedures_var = vars_oracle[3]
                tables_var = vars_oracle[4]
                synonym_var = vars_oracle[5]
                constraints_var = vars_oracle[6]
                packages_var = vars_oracle[7]
                triggers_var = vars_oracle[8]
                matviews_var = vars_oracle[9]
                views_var = vars_oracle[10]

                checked_boxes = sum(1 for var_oracle in vars_oracle if var_oracle.get() == 1)
                progressbar_increment = 100 / checked_boxes if checked_boxes else 0

                progressbar_oracle["value"] = 0

                if synonym_var.get():
                    synonym_thread = Thread(target=export_synonyms, args=(schema_name[0], queue1, author))
                    synonym_thread.start()

                if synonym_var.get():
                    while True:
                        if not synonym_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_synonym = queue1.get()
                            break

                        root.update()

                if synonym_var.get():
                    while True:
                        if result_synonym != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/synonyms.xml'/>\n")
                        break
                        root.update()

                if grants_var.get():
                    grants_thread = Thread(target=export_grants, args=(schema_name[0], queue2, selected_tab, author))
                    grants_thread.start()

                if grants_var.get():
                    while True:
                        if not grants_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_grants = queue2.get()
                            break

                        root.update()

                if grants_var.get():
                    while True:
                        if result_grants != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/grants.xml'/>\n")
                        break
                        root.update()

                if datatypes_var.get():
                    datatypes_thread = Thread(target=export_datatypes,
                                              args=(schema_name[0], queue3, selected_tab, author))
                    datatypes_thread.start()

                if datatypes_var.get():
                    while True:
                        if not datatypes_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_datatypes = queue3.get()
                            break

                        root.update()

                if datatypes_var.get():
                    while True:
                        if result_datatypes != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/datatypes.xml'/>\n")
                        break
                        root.update()

                if functions_var.get():
                    functions_thread = Thread(target=export_functions,
                                              args=(schema_name[0], queue4, selected_tab, author))
                    functions_thread.start()

                if functions_var.get():
                    while True:
                        if not functions_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_functions = queue4.get()
                            break

                        root.update()

                if functions_var.get():
                    while True:
                        if result_functions != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/functions.xml'/>\n")
                        break
                        root.update()

                if procedures_var.get():
                    procedures_thread = Thread(target=export_procedures,
                                               args=(schema_name[0], queue5, selected_tab, author))
                    procedures_thread.start()

                if procedures_var.get():
                    while True:
                        if not procedures_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_procedures = queue5.get()
                            break

                        root.update()

                if procedures_var.get():
                    while True:
                        if result_procedures != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/procedures.xml'/>\n")

                        break
                        root.update()

                if tables_var.get():
                    tables_thread = Thread(target=export_tables, args=(schema_name[0], queue6, selected_tab, author))
                    tables_thread.start()

                if tables_var.get():
                    while True:
                        if not tables_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_tables = queue6.get()
                            break

                        root.update()

                if tables_var.get():
                    while True:
                        if result_tables != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/tables.xml'/>\n")
                        break
                        root.update()

                if constraints_var.get():
                    constraints_thread = Thread(target=export_constraints,
                                                args=(schema_name[0], queue11, selected_tab, author))
                    constraints_thread.start()

                if constraints_var.get():
                    while True:
                        if not constraints_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_constraints = queue11.get()
                            break

                        root.update()

                if constraints_var.get():
                    while True:
                        if result_constraints != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/constraints.xml'/>\n")

                        break
                        root.update()

                if packages_var.get():
                    packages_thread = Thread(target=export_packages,
                                             args=(schema_name[0], queue7, author))
                    packages_thread.start()

                if packages_var.get():

                    while True:
                        if not packages_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_packages = queue7.get()
                            break

                        root.update()

                if packages_var.get():
                    while True:
                        if result_packages != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/packages.xml'/>\n")

                        break
                        root.update()

                if triggers_var.get():
                    triggers_thread = Thread(target=export_triggers,
                                             args=(schema_name[0], queue8, selected_tab, author))
                    triggers_thread.start()

                if triggers_var.get():
                    while True:
                        if not triggers_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_triggers = queue8.get()
                            break

                        root.update()

                if triggers_var.get():
                    while True:
                        if result_triggers != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/triggers.xml'/>\n")
                        break
                        root.update()

                if views_var.get():
                    views_thread = Thread(target=export_views, args=(schema_name[0], queue9, selected_tab, author))
                    views_thread.start()

                if views_var.get():
                    while True:
                        if not views_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_views = queue9.get()
                            break

                        root.update()

                if views_var.get():
                    while True:
                        if result_views != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/views.xml'/>\n")

                        break
                        root.update()

                if matviews_var.get():
                    matviews_thread = Thread(target=export_matviews,
                                             args=(schema_name[0], queue10, selected_tab, author))
                    matviews_thread.start()

                if matviews_var.get():

                    while True:
                        if not matviews_thread.is_alive():
                            progressbar_oracle["value"] += progressbar_increment
                            result_matviews = queue10.get()
                            break

                        root.update()

                if matviews_var.get():
                    while True:
                        if result_matviews != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/matviews.xml'/>\n")
                        break
                        root.update()
            else:
                roles_var = vars_pg[0]
                grants_var = vars_pg[1]
                datatypes_var = vars_pg[2]
                functions_var = vars_pg[3]
                procedures_var = vars_pg[4]
                tables_var = vars_pg[5]
                constraints_var = vars_pg[6]
                triggers_var = vars_pg[7]
                matviews_var = vars_pg[8]
                views_var = vars_pg[9]

                checked_boxes = sum(1 for var_pg in vars_pg if var_pg.get() == 1)
                progressbar_increment = 100 / checked_boxes if checked_boxes else 0

                progressbar_pg["value"] = 0

                if roles_var.get():
                    roles_thread = Thread(target=export_roles, args=(schema_name[0], queue1, author))
                    roles_thread.start()

                if roles_var.get():
                    while True:
                        if not roles_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_roles = queue1.get()
                            break

                        root.update()

                if roles_var.get():
                    while True:
                        if result_roles != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/roles.xml'/>\n")
                        break
                        root.update()

                if grants_var.get():
                    grants_thread = Thread(target=export_grants, args=(schema_name[0], queue2, selected_tab, author))
                    grants_thread.start()

                if grants_var.get():
                    while True:
                        if not grants_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_grants = queue2.get()
                            break

                        root.update()

                if grants_var.get():
                    while True:
                        if result_grants != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/grants.xml'/>\n")
                        break
                        root.update()

                if datatypes_var.get():
                    datatypes_thread = Thread(target=export_datatypes,
                                              args=(schema_name[0], queue3, selected_tab, author))
                    datatypes_thread.start()

                if datatypes_var.get():
                    while True:
                        if not datatypes_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_datatypes = queue3.get()
                            break

                        root.update()

                if datatypes_var.get():
                    while True:
                        if result_datatypes != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/datatypes.xml'/>\n")
                        break
                        root.update()

                if functions_var.get():
                    functions_thread = Thread(target=export_functions,
                                              args=(schema_name[0], queue4, selected_tab, author))
                    functions_thread.start()

                if functions_var.get():
                    while True:
                        if not functions_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_functions = queue4.get()
                            break

                        root.update()

                if functions_var.get():
                    while True:
                        if result_functions != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/functions.xml'/>\n")
                        break
                        root.update()

                if procedures_var.get():
                    procedures_thread = Thread(target=export_procedures,
                                               args=(schema_name[0], queue5, selected_tab, author))
                    procedures_thread.start()

                if procedures_var.get():
                    while True:
                        if not procedures_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_procedures = queue5.get()
                            break

                        root.update()

                if procedures_var.get():
                    while True:
                        if result_procedures != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/procedures.xml'/>\n")

                        break
                        root.update()

                if tables_var.get():
                    tables_thread = Thread(target=export_tables, args=(schema_name[0], queue6, selected_tab, author))
                    tables_thread.start()

                if tables_var.get():
                    while True:
                        if not tables_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_tables = queue6.get()
                            break

                        root.update()

                if tables_var.get():
                    while True:
                        if result_tables != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/tables.xml'/>\n")
                        break
                        root.update()

                if constraints_var.get():
                    constraints_thread = Thread(target=export_constraints,
                                                args=(schema_name[0], queue7, selected_tab, author))
                    constraints_thread.start()

                if constraints_var.get():

                    while True:
                        if not constraints_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_constraints = queue7.get()
                            break

                        root.update()

                if constraints_var.get():
                    while True:
                        if result_constraints != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/constraints.xml'/>\n")

                        break
                        root.update()

                if triggers_var.get():
                    triggers_thread = Thread(target=export_triggers,
                                             args=(schema_name[0], queue8, selected_tab, author))
                    triggers_thread.start()

                if triggers_var.get():
                    while True:
                        if not triggers_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_triggers = queue8.get()
                            break

                        root.update()

                if triggers_var.get():
                    while True:
                        if result_triggers != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/triggers.xml'/>\n")
                        break
                        root.update()

                if views_var.get():
                    views_thread = Thread(target=export_views, args=(schema_name[0], queue9, selected_tab, author))
                    views_thread.start()

                if views_var.get():
                    while True:
                        if not views_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_views = queue9.get()
                            break

                        root.update()

                if views_var.get():
                    while True:
                        if result_views != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/views.xml'/>\n")

                        break
                        root.update()

                if matviews_var.get():
                    matviews_thread = Thread(target=export_matviews,
                                             args=(schema_name[0], queue10, selected_tab, author))
                    matviews_thread.start()

                if matviews_var.get():

                    while True:
                        if not matviews_thread.is_alive():
                            progressbar_pg["value"] += progressbar_increment
                            result_matviews = queue10.get()
                            break

                        root.update()

                if matviews_var.get():
                    while True:
                        if result_matviews != 0:
                            with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a",
                                      encoding='utf-8') as xml_file:
                                xml_file.write(
                                    f"\t<include file='{schema_name[0]}/matviews.xml'/>\n")
                        break
                        root.update()
        with open(f"{folder_path}/{schema_name[0]}/{schema_name[0]}.xml", "a", encoding='utf-8') as xml_file:
            xml_file.write("\n</databaseChangeLog>\n")
            xml_file.close()

    with open(f"{folder_path}/install.xml", "a", encoding='utf-8') as xml_file:
        xml_file.write("\n</databaseChangeLog>\n")
        xml_file.close()

    current_time_finish_export = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    if selected_tab == 'Oracle':
        log_listbox_oracle.insert(END, f"{current_time_finish_export}: Экспорт завершен успешно")
    else:
        log_listbox_pg.insert(END, f"{current_time_finish_export}: Экспорт завершен успешно")

    messagebox.showinfo("Успешно", "Выгрузка объектов схемы успешно завершена!")


def stop_threads():
    for thread in threading.enumerate():
        if thread is not threading.main_thread():
            thread.stop()


atexit.register(stop_threads)

root = Tk()

root.title("Экспорт объектов бд и создание скриптов миграции Liquibase")
root.resizable(False, False)
root.geometry("700x1250")

tab_control = ttk.Notebook(root)
pg_tab = ttk.Frame(tab_control)
oracle_tab = ttk.Frame(tab_control)

tab_control.add(pg_tab, text='Postgresql')
tab_control.add(oracle_tab, text='Oracle')

tab_control.pack(expand=1, fill='both')

labels_pg = ['Хост:', 'Порт:', 'Наименование БД:', 'Пользователь:', 'Пароль:', 'Автор миграции:']
defaults_pg = ['', '5432', '', 'postgres', 'postgres', 'nosenko']
show_pg = [None, None, None, None, "*", None]

entries_pg = []
variables_pg = []

info_frame_pg = LabelFrame(pg_tab, text="Информация о соединении")
info_frame_pg.pack(side='top', fill='x', padx=10, pady=10)
tab_control.pack(side='top', expand=True, fill='both')

for i, label_pg in enumerate(labels_pg):
    label_pg = Label(info_frame_pg, text=label_pg)
    label_pg.grid(row=i, column=0, pady=10, padx=10, sticky=E)

    variable_pg = StringVar()
    variable_pg.set(defaults_pg[i])

    entry = Entry(info_frame_pg, textvariable=variable_pg, show=show_pg[i])
    entry.grid(row=i, column=1, pady=10, padx=10, sticky=W)
    variables_pg.append(variable_pg)

tree_frame_pg = LabelFrame(pg_tab, text="Выбрать схемы для исключения экспорта")
tree_frame_pg.pack(side='top', fill='x', padx=10, pady=10)

export_exclude_scheme_pg = Button(info_frame_pg, text="Загрузить схемы",
                                  command=load_schema_for_exclude)
export_exclude_scheme_pg.grid(row=6, column=1, columnspan=2, pady=10, ipadx=10)

scrollbar_exclude_pg = Scrollbar(tree_frame_pg)
scrollbar_exclude_pg.grid(row=1, column=1, sticky=N + S)

tree_scheme_pg = Listbox(tree_frame_pg, selectmode=MULTIPLE, yscrollcommand=scrollbar_exclude_pg.set, width=107)
tree_scheme_pg.grid(row=1, column=2)

scrollbar_exclude_pg.config(command=tree_scheme_pg.yview)

checkboxes_pg = ['Экспорт ролей', 'Экспорт грантов', 'Экспорт типов данных', 'Экспорт функций', 'Экспорт процедур',
                 'Экспорт таблиц', 'Экспорт ограничений', 'Экспорт триггеров', 'Экспорт мат. представлений',
                 'Экспорт представлений']

checkbox_frame_pg = LabelFrame(pg_tab, text="Выбор объектов для экспорта", padx=35)
checkbox_frame_pg.pack(fill='x', padx=10, pady=10)

vars_pg = []

for i, checkbox_pg in enumerate(checkboxes_pg):
    var_pg = IntVar()
    Checkbutton(checkbox_frame_pg, text=checkbox_pg, variable=var_pg).grid(row=i, column=0, sticky=W, padx=10, pady=5)
    vars_pg.append(var_pg)

action_frame_pg = LabelFrame(pg_tab, text="Лог экспорта")
action_frame_pg.pack(fill='x', padx=10, pady=10)

export_button_pg = Button(checkbox_frame_pg, text="Экспорт объектов", command=export_selected_objects)
export_button_pg.grid(row=10, column=0, columnspan=2, pady=10, ipadx=10)

scrollbar_pg = Scrollbar(action_frame_pg)
scrollbar_pg.grid(row=0, column=1, sticky=N + S)

scrollbar1_pg = Scrollbar(action_frame_pg, orient=HORIZONTAL)
scrollbar1_pg.grid(row=1, column=0, sticky=W + E)

log_listbox_pg = Listbox(action_frame_pg, width=100, yscrollcommand=scrollbar_pg.set, xscrollcommand=scrollbar1_pg.set)
log_listbox_pg.grid(row=0, column=0, sticky=N + S + E + W)

scrollbar_pg.config(command=log_listbox_pg.yview)
scrollbar1_pg.config(command=log_listbox_pg.xview)

root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(0, weight=1)
action_frame_pg.grid_rowconfigure(0, weight=1)
action_frame_pg.grid_columnconfigure(0, weight=1)

progressbar_pg = ttk.Progressbar(action_frame_pg, orient="horizontal", mode="determinate", length=670)
progressbar_pg.grid(row=2, column=0, columnspan=2)

labels_oracle = ['Хост:', 'Порт:', 'Наименование БД:', 'Пользователь:', 'Пароль:', 'Автор миграции:']
defaults_oracle = ['', '1521', '', 'postgres', 'postgres', 'nosenko']
show_oracle = [None, None, None, None, "*", None]

entries_oracle = []
variables_oracle = []

info_frame_oracle = LabelFrame(oracle_tab, text="Информация о соединении")
info_frame_oracle.pack(side='top', fill='x', padx=10, pady=10)
tab_control.pack(side='top', expand=True, fill='both')

for i, label_oracle in enumerate(labels_oracle):
    label_oracle = Label(info_frame_oracle, text=label_oracle)
    label_oracle.grid(row=i, column=0, pady=10, padx=10, sticky=E)

    variable_oracle = StringVar()
    variable_oracle.set(defaults_oracle[i])

    entry = Entry(info_frame_oracle, textvariable=variable_oracle, show=show_oracle[i])
    entry.grid(row=i, column=1, pady=10, padx=10, sticky=W)
    variables_oracle.append(variable_oracle)

tree_frame_oracle = LabelFrame(oracle_tab, text="Выбрать схемы для исключения экспорта")
tree_frame_oracle.pack(side='top', fill='x', padx=10, pady=10)

export_exclude_scheme_oracle = Button(info_frame_oracle, text="Загрузить схемы",
                                      command=load_schema_for_exclude)
export_exclude_scheme_oracle.grid(row=6, column=1, columnspan=2, pady=10, ipadx=10)

scrollbar_exclude_oracle = Scrollbar(tree_frame_oracle)
scrollbar_exclude_oracle.grid(row=1, column=1, sticky=N + S)

tree_scheme_oracle = Listbox(tree_frame_oracle, selectmode=MULTIPLE, yscrollcommand=scrollbar_exclude_oracle.set, width=107)
tree_scheme_oracle.grid(row=1, column=2)

scrollbar_exclude_oracle.config(command=tree_scheme_oracle.yview)

checkboxes_oracle = ['Экспорт грантов', 'Экспорт типов данных', 'Экспорт функций', 'Экспорт процедур',
                     'Экспорт таблиц', 'Экспорт синонимов', 'Экспорт ограничений', 'Экспорт пакетов',
                     'Экспорт триггеров',
                     'Экспорт мат. представлений',
                     'Экспорт представлений']

checkbox_frame_oracle = LabelFrame(oracle_tab, text="Выбор объектов для экспорта", padx=35)
checkbox_frame_oracle.pack(fill='x', padx=10, pady=10)

vars_oracle = []

for i, checkbox_oracle in enumerate(checkboxes_oracle):
    var_oracle = IntVar()
    Checkbutton(checkbox_frame_oracle, text=checkbox_oracle, variable=var_oracle).grid(row=i, column=0, sticky=W,
                                                                                       padx=10, pady=5)
    vars_oracle.append(var_oracle)

action_frame_oracle = LabelFrame(oracle_tab, text="Лог экспорта")
action_frame_oracle.pack(fill='x', padx=10, pady=10)

export_button_oracle = Button(checkbox_frame_oracle, text="Экспорт объектов", command=export_selected_objects)
export_button_oracle.grid(row=11, column=0, columnspan=2, pady=10, ipadx=10)

scrollbar_oracle = Scrollbar(action_frame_oracle)
scrollbar_oracle.grid(row=0, column=1, sticky=N + S)

scrollbar1_oracle = Scrollbar(action_frame_oracle, orient=HORIZONTAL)
scrollbar1_oracle.grid(row=1, column=0, sticky=W + E)

log_listbox_oracle = Listbox(action_frame_oracle, width=100, yscrollcommand=scrollbar_oracle.set,
                             xscrollcommand=scrollbar1_oracle.set)
log_listbox_oracle.grid(row=0, column=0, sticky=N + S + E + W)

scrollbar_oracle.config(command=log_listbox_oracle.yview)
scrollbar1_oracle.config(command=log_listbox_oracle.xview)

root.grid_rowconfigure(0, weight=1)
root.grid_columnconfigure(0, weight=1)
action_frame_oracle.grid_rowconfigure(0, weight=1)
action_frame_oracle.grid_columnconfigure(0, weight=1)

progressbar_oracle = ttk.Progressbar(action_frame_oracle, orient="horizontal", mode="determinate", length=670)
progressbar_oracle.grid(row=2, column=0, columnspan=2)

root.mainloop()
