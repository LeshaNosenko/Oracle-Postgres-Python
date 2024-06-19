import asyncio
import os
import tempfile
import threading

import psycopg2
from aiogram.client.session import aiohttp
from flask import Flask, render_template, request, send_file
import zipfile
import shutil
from aiogram import Bot, Dispatcher, types
from aiogram.fsm.context import FSMContext
from aiogram.filters.command import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, WebAppInfo

app = Flask( __name__ )

global scheme
global selected_object


def crete_directory_if_not_exists(directory):
    if not os.path.exists(directory):
        os.makedirs(directory)

def export_functions(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute(
        f"SELECT routine_name, routine_type, routine_definition FROM information_schema.routines WHERE routine_type='FUNCTION' AND routine_schema='{selected_scheme}'" )

    functions = cur.fetchall()
    if not functions:
        return

    temp_folder = tempfile.mkdtemp()

    crete_directory_if_not_exists(temp_folder )

    with open( f"{temp_folder}/functions.xml", "w", encoding = 'utf-8' ) as xml_file:
        xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
        for func in functions:
            xml_file.write(
                f"\t<changeSet author='{author_pg}' id='create_{func [0]}' context='all' runOnChange='true'>\n" )
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/functions/{func [0]}.sql' splitStatements='false'/>\n" )
            xml_file.write( "\t</changeSet>\n" )
            xml_file.write( "</databaseChangeLog>\n" )
            cur.execute( f"SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = '{func [0]}' and prokind = 'f'" )
            function_def = cur.fetchall() [0]
            with open( f"{temp_folder}/{func [0]}.sql", "w", encoding = 'utf-8' ) as func_file:
                func_file.write( function_def [0] )
    return temp_folder


def export_roles(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute(
        "SELECT 'CREATE ROLE \"'||r.rolname||'\" WITH  PASSWORD '''||r.rolname||''' '||case when rolcanlogin is true then 'LOGIN' else 'NOLOGIN' end ||' '||case when rolsuper is true then 'SUPERUSER' else 'NOSUPERUSER' end ||' '||case when rolinherit is true then 'INHERIT' else 'NOINHERIT' end ||'  '||case when rolcreatedb is true then 'CREATEDB' else 'NOCREATEDB' end ||' '||case when rolcreaterole is true then 'CREATEROLE' else 'NOCREATEROLE' end ||' '||case when rolreplication is true then 'REPLICATION' else 'NOREPLICATION' end ||' VALID UNTIL ''infinity''; ' as a, r.rolname from ( select r.* , ARRAY(SELECT b.rolname FROM pg_catalog.pg_auth_members m JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) WHERE m.member = r.oid) as memberof FROM pg_roles r where r.rolname not like 'pg_%' order by r.rolname) r" )
    roles = cur.fetchall()

    cur.execute( f"SELECT 'CREATE EXTENSION IF NOT EXISTS ' as a, extname as b FROM pg_extension" )
    extensions = cur.fetchall()

    if not roles:
        return

    temp_folder = tempfile.mkdtemp()

    crete_directory_if_not_exists(temp_folder )

    with open( f"{temp_folder}/roles.xml", "w", encoding = 'utf-8' ) as xml_file:
        xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
        xml_file.write(
            f"\t<changeSet author='{author_pg}' id='create_roles' context='all' runOnChange='true'>\n" )
        xml_file.write(
            f"\t\t<sqlFile path='{selected_scheme}/roles.sql' splitStatements='false'/>\n" )
        xml_file.write( "\t</changeSet>\n" )
        with open( f"{temp_folder}/roles.sql", "a", encoding = 'utf-8' ) as role_file:
            for extension in extensions:
                role_file.write(
                    f"do\n$$\nbegin {extension [0]} \"{extension [1]}\";\n end;\n$$;\n\n" )
        with open( f"{temp_folder}/roles.sql", "a", encoding = 'utf-8' ) as role_file:
            for role in roles:
                role_file.write(
                    f"do\n$$\nbegin\nif not exists (select * from pg_roles where rolname = '{role [1]}') then {role [0]}\n end if;\nend;\n$$;\n\n" )
        xml_file.write( "</databaseChangeLog>\n" )
    return temp_folder


def export_grants(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute( f"SELECT schema_name FROM information_schema.schemata WHERE schema_name = '{selected_scheme}'" )

    grants = cur.fetchall()

    if not grants:
        return

    temp_folder = tempfile.mkdtemp()

    crete_directory_if_not_exists(temp_folder )

    with open( f"{temp_folder}/grants.xml", "w", encoding = 'utf-8' ) as xml_file:
        xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
        xml_file.write(
            f"\t<changeSet author='{author_pg}' id='grants' context='all' runOnChange='true'>\n" )
        xml_file.write(
            f"\t\t<sqlFile path='{selected_scheme}/grants/grants.sql' splitStatements='false'/>\n" )
        xml_file.write( "\t</changeSet>\n" )
        xml_file.write( "</databaseChangeLog>\n" )
        with open( f"{temp_folder}/grants.sql", "w", encoding = 'utf-8' ) as grants_file:
            try:
                cur.execute(
                    f"SELECT distinct c.grantee, c.privilege_type, t.sequence_name FROM information_schema.usage_privileges c join information_schema.sequences t on t.sequence_name = c.object_name and c.object_type = 'SEQUENCE' and c.object_schema='{selected_scheme}'" )
                grants = cur.fetchall()
                for grant in grants:
                    grantee = grant [0]
                    privilege = grant [1]
                    object_name = grant [2]
                    grants_file.write(
                        f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n" )
            except psycopg2.Error as e:
                print( f"Error executing sequence grants query: {e}" )
            try:
                cur.execute(
                    f"SELECT distinct c.grantee, c.privilege_type, c.routine_name FROM information_schema.routine_privileges c where exists(select 1 from information_schema.routines t where t.routine_name = c.routine_name and t.routine_schema='{selected_scheme}' and t.routine_type = 'FUNCTION')" )
                grants = cur.fetchall()
                for grant in grants:
                    grantee = grant [0]
                    privilege = grant [1]
                    object_name = grant [2]
                    grants_file.write(
                        f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n" )
            except psycopg2.Error as e:
                print( f"Error executing sequence grants query: {e}" )
            try:
                cur.execute(
                    f"SELECT distinct c.grantee, c.privilege_type, c.routine_name FROM information_schema.routine_privileges c where exists(select 1 from information_schema.triggers t where t.trigger_name = c.routine_name) and c.routine_schema='{selected_scheme}'" )
                grants = cur.fetchall()
                for grant in grants:
                    grantee = grant [0]
                    privilege = grant [1]
                    object_name = grant [2]
                    grants_file.write(
                        f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n" )
            except psycopg2.Error as e:
                print( f"Error executing sequence grants query: {e}" )
            try:
                cur.execute(
                    f"SELECT distinct c.grantee, c.privilege_type, c.routine_name FROM information_schema.routine_privileges c where exists(select 1 from information_schema.routines t where t.routine_name = c.routine_name and t.routine_schema='{selected_scheme}' and t.routine_type = 'PROCEDURE')" )
                grants = cur.fetchall()
                for grant in grants:
                    grantee = grant [0]
                    privilege = grant [1]
                    object_name = grant [2]
                    grants_file.write(
                        f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n" )
            except psycopg2.Error as e:
                print( f"Error executing sequence grants query: {e}" )
            try:
                cur.execute(
                    f"SELECT distinct c.grantee, c.privilege_type, c.table_name  FROM information_schema.table_privileges c where exists(select 1 from information_schema.views t where t.table_name = c.table_name) and c.table_schema='{selected_scheme}'" )
                grants = cur.fetchall()
                for grant in grants:
                    grantee = grant [0]
                    privilege = grant [1]
                    object_name = grant [2]
                    grants_file.write(
                        f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n" )
            except psycopg2.Error as e:
                print( f"Error executing sequence grants query: {e}" )
            try:
                cur.execute(
                    f"SELECT distinct coalesce(nullif(s[1], ''), '{selected_scheme}') grantee, CASE ch WHEN 'r' THEN 'SELECT' WHEN 'w' THEN 'UPDATE' WHEN 'a' THEN 'INSERT' WHEN 'd' THEN 'DELETE' WHEN 'D' THEN 'TRUNCATE' WHEN 'x' THEN 'REFERENCES' WHEN 't' THEN 'TRIGGER' END AS privilege,relname FROM  pg_class  JOIN pg_namespace ON pg_namespace.oid = relnamespace JOIN pg_roles ON pg_roles.oid = relowner, unnest(coalesce(relacl::text[], format('{{%s=arwdDxt/%s}}', rolname, rolname)::text[])) AS acl, regexp_split_to_array(acl, '=|/') AS s, regexp_split_to_table(s[2], '') ch WHERE nspname ='{selected_scheme}'" )
                grants = cur.fetchall()
                for grant in grants:
                    grantee = grant [0]
                    privilege = grant [1]
                    object_name = grant [2]
                    grants_file.write(
                        f"GRANT {privilege} ON {selected_scheme}.{object_name} TO {grantee};\n" )
            except psycopg2.Error as e:
                print( f"Error executing sequence grants query: {e}" )
    return temp_folder


def export_datatypes(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute( """
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
        	""" )

    datatypes = cur.fetchall()

    if not datatypes:
        return

    temp_folder = tempfile.mkdtemp()

    crete_directory_if_not_exists(temp_folder )

    for datatype in datatypes:
        with open( f"{temp_folder}/{datatype [1]}.sql", "w",
                   encoding = 'utf-8' ) as sql_file:
            if datatype [2]:
                values = f"AS ENUM ({datatype [2]})"
            elif datatype [3]:
                values = f"AS ({datatype [3]})"
            else:
                values = ""
            sql_file.write(
                f"do\n$$\nbegin\nif not exists (select * from pg_type where typname = '{datatype [1]}') then CREATE TYPE {datatype [1]} {values};\n end if;\nend;\n$$;\n\n" )
        sql_file.close()

        with open( f"{temp_folder}/datatypes.xml", "w", encoding = 'utf-8' ) as xml_file:
            xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
            xml_file.write(
                " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
            for datatype in datatypes:
                xml_file.write(
                    f"\t<changeSet author='{author_pg}' id='create_{datatype [1]}' context='all' runOnChange='true'>\n" )
                xml_file.write(
                    f"\t\t<sqlFile path='{selected_scheme}/datatypes/{datatype [1]}.sql' splitStatements='false'/>\n" )
                xml_file.write( "\t</changeSet>\n" )
            xml_file.write( "</databaseChangeLog>\n" )
    return temp_folder


def export_procedures(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute(
        f"SELECT routine_name, routine_type, routine_definition FROM information_schema.routines WHERE routine_type='PROCEDURE' AND routine_schema='{selected_scheme}'" )

    procedures = cur.fetchall()
    if not procedures:
        return

    temp_folder = tempfile.mkdtemp()

    crete_directory_if_not_exists(temp_folder )

    with open( f"{temp_folder}/procedures.xml", "w", encoding = 'utf-8' ) as xml_file:
        xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
        for proc in procedures:
            xml_file.write(
                f"\t<changeSet author='{author_pg}' id='create_{proc [0]}' context='all' runOnChange='true'>\n" )
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/procedures/{proc [0]}.sql' splitStatements='false'/>\n" )
            xml_file.write( "\t</changeSet>\n" )
            xml_file.write( "</databaseChangeLog>\n" )
            cur.execute( f"SELECT pg_get_functiondef(oid) FROM pg_proc WHERE proname = '{proc [0]}'" )
            procedure_def = cur.fetchall() [0]
            with open( f"{temp_folder}/{proc [0]}.sql", "w", encoding = 'utf-8' ) as proc_file:
                proc_file.write( procedure_def [0] )
    return temp_folder


def export_tables(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute(
        f"SELECT table_name FROM information_schema.tables WHERE table_schema='{selected_scheme}'" )

    tables = cur.fetchall()

    if not tables:
        return

    temp_folder_table = tempfile.mkdtemp()

    if not os.path.exists( f"{temp_folder_table}/tables/" ):
        os.makedirs( f"{temp_folder_table}/tables/" )

    with open( f"{temp_folder_table}/tables.xml", "w", encoding = 'utf-8' ) as xml_file:
        xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        xml_file.write(
            "<databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
        for table in tables:
            xml_file.write(
                f"<changeSet author='{author_pg}' id='create_{table [0]}' context='all' runOnChange='true'>\n" )
            xml_file.write(
                f"<sqlFile path='{selected_scheme}/tables/{table [0]}.sql' splitStatements='true' endDelimiter=';'/>\n" )
            xml_file.write( "</changeSet>\n" )
            cur.execute(
                f"SELECT column_name, udt_name, is_nullable, case when column_default like '%_seq%' then null else column_default end, ordinal_position::int FROM information_schema.columns WHERE table_name='{table [0]}' and table_schema = '{selected_scheme}'" )
            columns = cur.fetchall()
            if columns and columns [0] is not None:
                with open( f"{temp_folder_table}/tables/{table [0]}.sql", "a",
                           encoding = 'utf-8' ) as sql_file:
                    sql_file.write( f"CREATE TABLE IF NOT EXISTS {selected_scheme}.{table [0]} (\n" )
                    for i, column in enumerate( columns ):
                        sql_file.write( f"\"{column [0]}\" {column [1]}" )
                        if column [2] == 'NO':
                            sql_file.write( " NOT NULL" )
                            if column [3] is not None and column [0] != 'id':
                                sql_file.write( f" DEFAULT {column [3]}" )
                        if i != len( columns ) - 1:
                            sql_file.write( "," )
                        sql_file.write( "\n" )
                    if i != len( columns ) - 1:
                        sql_file.write( "," )
                    cur.execute(
                        f"SELECT pg_get_constraintdef(pg_constraint.oid) as constraint_definition, pg_constraint.conname as constraint_name FROM pg_constraint JOIN pg_class ON pg_constraint.conrelid = pg_class.oid WHERE pg_constraint.contype = 'p' AND pg_class.relname = '{table [0]}' limit 1" )
                    pk_keys = cur.fetchall()
                    if pk_keys is not None:
                        for pk_key in pk_keys:
                            sql_file.write( f",CONSTRAINT {pk_key [1]} {pk_key [0]}" )
                    sql_file.write( ");\n" )
                    sql_file.write( "\n" )

            cur.execute(
                f"SELECT obj_description((c.table_schema || '.' || c.table_name)::regclass) FROM information_schema.tables c WHERE c.table_name ='{table [0]}'" )
            table_comment = cur.fetchone()
            if table_comment is not None and table_comment [0] is not None:
                comment = table_comment [0].replace( "(", '' ).replace( ",)", '' )
                with open( f"{temp_folder_table}/tables/{table [0]}.sql", "a",
                           encoding = 'utf-8' ) as sql_file:
                    sql_file.write(
                        f"COMMENT ON TABLE {selected_scheme}.{table [0]} IS '{comment}';\n\n" )
                for column in columns:
                    cur.execute(
                        f"SELECT col_description(oid,'{column [4]}') FROM pg_class WHERE relname='{table [0]}'" )
                    column_comment = cur.fetchone()
                    if column_comment and column_comment [0] is not None:
                        comment = column_comment [0].replace( "(", '' ).replace( ",)", '' )
                        with open( f"{temp_folder_table}/tables/{table [0]}.sql",
                                   "a", encoding = 'utf-8' ) as sql_file:
                            sql_file.write(
                                f"COMMENT ON COLUMN {selected_scheme}.{table [0]}.\"{column [0]}\" IS '{comment}';\n" )
                cur.execute(
                    f"Select i.relname, pg_get_indexdef(ix.indexrelid) from pg_class t, pg_class i, pg_index ix, pg_attribute a where t.oid = ix.indrelid  and i.oid = ix.indexrelid  and a.attrelid = t.oid  and a.attnum = ANY(ix.indkey)  and t.relkind = 'r'  and t.relname = '{table [0]}' AND t.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{selected_scheme}')" )
                indexes = cur.fetchall()
                with open( f"{temp_folder_table}/tables/{table [0]}.sql", "a",
                           encoding = 'utf-8' ) as sql_file:
                    sql_file.write( "\n" )
                    for index in indexes:
                        index_def = index [1]
                        if "UNIQUE" in index_def:
                            index_def = index_def.replace( "UNIQUE INDEX",
                                                           "UNIQUE INDEX IF NOT EXISTS" )
                            sql_file.write( index_def )
                            sql_file.write( ";" )
                            sql_file.write( '\n' )
                        elif "CREATE INDEX" in index_def:
                            index_def = index_def.replace( "CREATE INDEX",
                                                           "CREATE INDEX IF NOT EXISTS" )
                            sql_file.write( index_def )
                            sql_file.write( ";" )
                            sql_file.write( '\n' )

    with open( f"{temp_folder_table}/tables.xml", "a", encoding = 'utf-8') as xml_file:
        xml_file.write( "</databaseChangeLog>\n" )

    return temp_folder_table


def export_constraints(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute(
        f"SELECT pg_class.relname as table_name, pg_get_constraintdef(pg_constraint.oid) as constraint_definition, pg_constraint.conname as constraint_name FROM pg_constraint JOIN pg_class ON pg_constraint.conrelid = pg_class.oid WHERE pg_constraint.contype = 'f' AND pg_class.relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = '{selected_scheme}')" )

    constraints = cur.fetchall()

    if not constraints:
        return

    temp_folder = tempfile.mkdtemp()

    crete_directory_if_not_exists(temp_folder )

    try:
        with open( f"{temp_folder}/constraints.xml", "w", encoding = 'utf-8' ) as xml_file:
            xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
            xml_file.write(
                "<databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
            for constraint in constraints:
                xml_file.write(
                    f"\t<changeSet author='{author_pg}' id='create_{constraint [2]}' context='all' runOnChange='true'>\n" )
                xml_file.write(
                    f"\t\t<sqlFile path='{selected_scheme}/constraints/{constraint [2]}.sql' splitStatements='false'/>\n" )
                xml_file.write( "\t</changeSet>\n" )
                with open( f"{temp_folder}/{constraint [2]}.sql",
                           "w", encoding = 'utf-8' ) as constraint_file:
                    constraint_file.write(
                        f"do $$\nbegin\nexecute 'ALTER TABLE {selected_scheme}.{constraint [2]} drop constraint if exists {constraint [1]}';\n" )
                    constraint_file.write(
                        f"execute 'ALTER TABLE {selected_scheme}.{constraint [2]} ADD CONSTRAINT {constraint [1]} {constraint [0]}';\n" )
                    constraint_file.write(
                        "exception when others then null;\n" )
                    constraint_file.write(
                        "end $$;" )
        with open( f"{temp_folder}/constraints.xml", "a", encoding = 'utf-8' ) as xml_file:
            xml_file.write( "</databaseChangeLog>\n" )
        return temp_folder

    except:
        return



def export_triggers(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    temp_folder = tempfile.mkdtemp()

    cur.execute(
        f"SELECT trigger_name, 'CREATE TRIGGER ' || trigger_name || ' ' || action_timing || ' ' || string_agg(event_manipulation, ' OR ') || ' ON ' || event_object_table || ' FOR EACH ROW ' || action_statement || ';' as ddl, event_object_table FROM information_schema.triggers WHERE event_object_schema='{selected_scheme}' GROUP BY trigger_name, action_timing, event_object_table, action_statement;" )
    triggers = cur.fetchall()

    if not triggers:
        return

    crete_directory_if_not_exists(temp_folder )

    with open( f"{temp_folder}/triggers.xml", "w", encoding = 'utf-8' ) as xml_file:
        xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
        for trigger in triggers:
            xml_file.write(
                f"\t<changeSet author='{author_pg}' id='create_{trigger [0]}' context='all' runOnChange='true'>\n" )
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/triggers/{trigger [0]}.sql' splitStatements='false'/>\n" )
            xml_file.write( "\t</changeSet>\n" )
        xml_file.write( "</databaseChangeLog>\n" )

    for trigger in triggers:
        with open( f"{temp_folder}/{trigger [0]}.sql", "w", encoding = 'utf-8' ) as sql_file:
            sql_file.write(
                f"DO $$ BEGIN IF EXISTS (SELECT * FROM information_schema.triggers WHERE event_object_schema = '{selected_scheme}' AND trigger_name = '{trigger [0]}') THEN EXECUTE 'DROP TRIGGER IF EXISTS {trigger [0]} ON {trigger [2]}'; EXECUTE '{trigger [1]}\n''; END IF; END $$;\n\n" )
        sql_file.close()
    return temp_folder


def export_matviews(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute( f"SELECT matviewname FROM pg_matviews WHERE schemaname='{selected_scheme}'" )

    matviews = cur.fetchall()
    if not matviews:
        return

    temp_folder = tempfile.mkdtemp()

    crete_directory_if_not_exists(temp_folder )

    with open( f"{temp_folder}/matviews.xml", "w", encoding = 'utf-8' ) as xml_file:
        xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
        for matview in matviews:
            matview_name = matview [0]
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='{selected_scheme}' AND table_name='{matview_name}');" )
            if not cur.fetchone() [0]:
                continue

            xml_file.write(
                f"\t<changeSet author='{author_pg}' id='create_{matview_name}' context='all' runOnChange='true'>\n" )
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/matviews/{matview_name}.sql' splitStatements='false'/>\n" )
            xml_file.write( "\t</changeSet>\n" )

            cur.execute( f"SELECT pg_get_viewdef('{selected_scheme}.{matview_name}', true)" )

            view_query = cur.fetchone()

            with open( f"{temp_folder}/{matview_name}.sql", "w",
                       encoding = 'utf-8' ) as sql_file:
                view_query = view_query [0].replace( "'", "" )
                sql_file.write(
                    f"CREATE MATERIALIZED VIEW IF NOT EXISTS {selected_scheme}.{matview_name} AS \n {view_query}" )
        xml_file.write( "</databaseChangeLog>\n" )
    return temp_folder


def export_views(selected_scheme, host_pg, port_pg, db_name_pg, user_pg, password_pg, author_pg):
    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute( f"SELECT table_name FROM information_schema.views WHERE table_schema='{selected_scheme}'" )

    views = cur.fetchall()

    if not views:
        return

    temp_folder = tempfile.mkdtemp()

    crete_directory_if_not_exists(temp_folder )

    with open( f"{temp_folder}/views.xml", "w", encoding = 'utf-8' ) as xml_file:
        xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        xml_file.write(
            " <databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n" )
        for view in views:
            view_name = view [0]
            cur.execute(
                f"SELECT EXISTS (SELECT 1 FROM information_schema.views WHERE table_schema='{selected_scheme}' AND table_name='{view_name}');" )
            if not cur.fetchone() [0]:
                continue

            xml_file.write(
                f"\t<changeSet author='{author_pg}' id='create_{view_name}' context='all' runOnChange='true'>\n" )
            xml_file.write(
                f"\t\t<sqlFile path='{selected_scheme}/views/{view_name}.sql' splitStatements='false'/>\n" )
            xml_file.write( "\t</changeSet>\n" )

            cur.execute( f"SELECT pg_get_viewdef('{selected_scheme}.{view_name}', true)" )

            view_query = cur.fetchone()

            with open( f"{temp_folder}/{view_name}.sql", "w", encoding = 'utf-8' ) as sql_file:
                view_query = view_query [0].replace( "'", "" )
                sql_file.write(
                    f"CREATE OR REPLACE VIEW IF NOT EXISTS {selected_scheme}.{view_name} AS \n {view_query}" )
        xml_file.write( "</databaseChangeLog>\n" )
    return temp_folder


@app.route( '/' )
def index():
    return render_template( 'index.html' )


@app.route( '/export_pg', methods = ['POST'] )
def export():
    host_pg = request.form.get( 'host_pg' )
    port_pg = request.form.get( 'port_pg' )
    db_name_pg = request.form.get( 'db_name_pg' )
    user_pg = request.form.get( 'user_pg' )
    password_pg = request.form.get( 'password_pg' )
    author_pg = request.form.get( 'author_pg' )
    export_roles_check = request.form.get( 'export_roles' )
    export_grants_check = request.form.get( 'export_grants' )
    export_functions_check = request.form.get( 'export_functions' )
    export_procedures_check = request.form.get( 'export_procedures' )
    export_datatypes_check = request.form.get( 'export_datatypes' )
    export_tables_check = request.form.get( 'export_tables' )
    export_constraints_check = request.form.get( 'export_constraints' )
    export_triggers_check = request.form.get( 'export_triggers' )
    export_matviews_check = request.form.get( 'export_matviews' )
    export_views_check = request.form.get( 'export_views' )

    conn = psycopg2.connect(
        host = host_pg,
        port = port_pg,
        database = db_name_pg,
        user = user_pg,
        password = password_pg )

    cur = conn.cursor()

    cur.execute(
        "SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT LIKE 'pg_%' AND schema_name NOT LIKE 'information_schema'" )
    all_schemes = cur.fetchall()

    common_temp_folder = tempfile.mkdtemp()

    install_xml_path = os.path.join( common_temp_folder, 'install.xml' )
    with open( install_xml_path, "w", encoding = 'utf-8' ) as install_xml_file:
        install_xml_file.write( "<?xml version='1.0' encoding='UTF-8'?>\n" )
        install_xml_file.write( "<databaseChangeLog xmlns='http://www.liquibase.org/xml/ns/dbchangelog'\n" )
        install_xml_file.write( "xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance'\n" )
        install_xml_file.write( "xsi:schemaLocation='http://www.liquibase.org/xml/ns/dbchangelog\n" )
        install_xml_file.write( "http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-3.5.xsd'>\n\n" )

    for schema_name in all_schemes:
        schema_folder = os.path.join( common_temp_folder, schema_name [0] )
        crete_directory_if_not_exists( schema_folder )

        if export_roles_check == 'on':
            roles_folder = export_roles( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                         author_pg )
            if roles_folder:
                shutil.copytree( roles_folder, os.path.join( schema_folder, 'roles' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/roles/roles.xml'/>\n" )

        if export_grants_check == 'on':
            grants_folder = export_grants( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                           author_pg )
            if grants_folder:
                shutil.copytree( grants_folder, os.path.join( schema_folder, 'grants' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/grants/grants.xml'/>\n" )

        if export_datatypes_check == 'on':
            datatypes_folder = export_datatypes( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                                 author_pg )
            if datatypes_folder:
                shutil.copytree( datatypes_folder, os.path.join( schema_folder, 'datatypes' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/datatypes/datatypes.xml'/>\n" )

        if export_functions_check == 'on':
            functions_folder = export_functions( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                                 author_pg )
            if functions_folder:
                shutil.copytree( functions_folder, os.path.join( schema_folder, 'functions' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/functions/functions.xml'/>\n" )

        if export_procedures_check == 'on':
            procedures_folder = export_procedures( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                                   author_pg )
            if procedures_folder:
                shutil.copytree( procedures_folder, os.path.join( schema_folder, 'procedures' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/procedures/procedures.xml'/>\n" )

        if export_tables_check == 'on':
            tables_folder = export_tables( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                           author_pg )
            if tables_folder:
                shutil.copytree( tables_folder, os.path.join( schema_folder, 'tables' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/tables/tables.xml'/>\n" )

        if export_constraints_check == 'on':
            constraints_folder = export_constraints( schema_name [0], host_pg, port_pg, db_name_pg, user_pg,
                                                     password_pg, author_pg )
            if constraints_folder:
                shutil.copytree( constraints_folder, os.path.join( schema_folder, 'constraints' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/constraints/constraints.xml'/>\n" )

        if export_triggers_check == 'on':
            triggers_folder = export_triggers( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                               author_pg )
            if triggers_folder:
                shutil.copytree( triggers_folder, os.path.join( schema_folder, 'triggers' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/triggers/triggers.xml'/>\n" )

        if export_matviews_check == 'on':
            matviews_folder = export_matviews( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                               author_pg )
            if matviews_folder:
                shutil.copytree( matviews_folder, os.path.join( schema_folder, 'matviews' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/matviews/matviews.xml'/>\n" )

        if export_views_check == 'on':
            views_folder = export_views( schema_name [0], host_pg, port_pg, db_name_pg, user_pg, password_pg,
                                         author_pg )
            if views_folder:
                shutil.copytree( views_folder, os.path.join( schema_folder, 'views' ) )
                with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
                    install_xml_file.write( f"\t<include file='{schema_name [0]}/views/views.xml'/>\n" )

    with open( install_xml_path, "a", encoding = 'utf-8' ) as install_xml_file:
        install_xml_file.write( "\n</databaseChangeLog>\n" )

    combined_zip_path = os.path.join( tempfile.gettempdir(), f"{db_name_pg}.zip" )
    with zipfile.ZipFile( combined_zip_path, 'w', zipfile.ZIP_DEFLATED ) as combined_zip:
        for root, dirs, files in os.walk( common_temp_folder ):
            for file in files:
                file_path = os.path.join( root, file )
                zip_path = os.path.relpath( file_path, common_temp_folder )
                combined_zip.write( file_path, zip_path )

    shutil.rmtree( common_temp_folder )

    return send_file( combined_zip_path, as_attachment = True )


if __name__ == '__main__':
    app.run( host = '0.0.0.0', port = 80, debug = True )
